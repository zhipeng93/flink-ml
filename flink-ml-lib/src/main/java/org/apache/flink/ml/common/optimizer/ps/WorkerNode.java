package org.apache.flink.ml.common.optimizer.ps;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.common.feature.LabeledLargePointWithWeight;
import org.apache.flink.ml.common.lossfunc.LossFunc;
import org.apache.flink.ml.common.optimizer.PSFtrl.FTRLParams;
import org.apache.flink.ml.common.optimizer.ps.datastorage.DenseLongVectorStorage;
import org.apache.flink.ml.common.optimizer.ps.datastorage.SparseLongDoubleVectorStorage;
import org.apache.flink.ml.common.optimizer.ps.message.MessageUtils;
import org.apache.flink.ml.common.optimizer.ps.message.PulledModelM;
import org.apache.flink.ml.linalg.SparseLongDoubleVector;
import org.apache.flink.ml.regression.linearregression.LinearRegression;
import org.apache.flink.ml.util.Bits;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.commons.collections.IteratorUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The worker. It does the following things: - caches the training data from input table. - Samples
 * a minibatch of training data and compute indices needed. - Pull model data from servers. - Push
 * gradients to servers.
 */
public class WorkerNode extends AbstractStreamOperator<Tuple2<Integer, byte[]>>
        implements TwoInputStreamOperator<
                        LabeledLargePointWithWeight, byte[], Tuple2<Integer, byte[]>>,
                IterationListener<Tuple2<Integer, byte[]>> {
    /** Optimizer-related parameters. */
    private final FTRLParams params;

    /** The loss function to optimize. */
    private final LossFunc lossFunc;

    /** The outputTag to output the model data when iteration ends. */
    // private final OutputTag <DenseVector> modelDataOutputTag;

    /** The cached training data. */
    private List<LabeledLargePointWithWeight> trainData;

    private ListState<LabeledLargePointWithWeight> trainDataState;

    /** The start index (offset) of the next mini-batch data for training. */
    private int batchOffSet = 0;

    private ListState<Integer> batchOffsetState;

    private List<LabeledLargePointWithWeight> batchTrainData;
    private ListState<LabeledLargePointWithWeight> batchTrainDataState;

    private long[] sortedBatchIndices;

    /** The partitioner of the model. */
    private Map<Integer, RangeModelPartitioner> partitioners;

    private ListState<Map<Integer, RangeModelPartitioner>> partitionersState;

    /**
     * The double array to sync among all workers. For example, when training {@link
     * LinearRegression}, this double array consists of [modelUpdate, totalWeight, totalLoss].
     */
    private byte[] feedback;

    private ListState<byte[]> feedbackState;

    /** The batch size on this partition. */
    private int localBatchSize;

    private int workerId;

    private PSAgent psAgent;

    int numPss;

    int nextModelId = 0;

    long lastIterationTime = System.currentTimeMillis();

    public WorkerNode(LossFunc lossFunc, FTRLParams params, int numPss) {
        this.lossFunc = lossFunc;
        this.params = params;
        this.numPss = numPss;
    }

    @Override
    public void open() {
        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        localBatchSize = params.globalBatchSize / numTasks;
        if (params.globalBatchSize % numTasks > taskId) {
            localBatchSize++;
        }
        this.workerId = getRuntimeContext().getIndexOfThisSubtask();
        this.psAgent = new PSAgent(workerId, output);
    }

    /** Samples a batch of training data and push the needed model index to ps. */
    private void sampleDataAndSendPullRequest(int epochWatermark) {
        // also sample a batch of training data and push the needed sparse models to ps.
        // clear the last batch of training data.
        int modelId = 0;
        batchTrainData.clear();
        for (int i = batchOffSet;
                i < Math.min(trainData.size(), batchOffSet + localBatchSize);
                i++) {
            LabeledLargePointWithWeight dataPoint = trainData.get(i);
            batchTrainData.add(dataPoint);
        }
        sortedBatchIndices = getSortedIndicesFromData(batchTrainData);
        batchOffSet += localBatchSize;
        batchOffSet = batchOffSet < trainData.size() ? batchOffSet : 0;
        // if (workerId == 0) {
        //    LOG.error(
        //        "[Worker-{}][iteration-{}] Sending pull-model request to servers, with {} nnzs.",
        //        workerId,
        //        epochWatermark,
        //        sortedIndices.length);
        // }
        psAgent.sparsePullModel(modelId, new DenseLongVectorStorage(sortedBatchIndices));
    }

    private static long[] getSortedIndicesFromData(List<LabeledLargePointWithWeight> dataPoints) {
        // HashSet<Long> indices = new HashSet();
        LongOpenHashSet indices = new LongOpenHashSet(); // flame graph from 12% to 9%.
        // Set<Long> indices = new HashSet<>();
        for (LabeledLargePointWithWeight dataPoint : dataPoints) {
            // Preconditions.checkState(
            //        dataPoint.features instanceof SparseLongDoubleVector,
            //        "Dense Vector" + "will be supported by dense pull.");
            long[] notZeros = dataPoint.features.indices;
            for (long index : notZeros) {
                indices.add(index);
            }
        }

        long[] sortedIndices = new long[indices.size()];
        Iterator<Long> iterator = indices.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            sortedIndices[i++] = iterator.next();
        }
        Arrays.sort(sortedIndices);
        return sortedIndices;
    }

    /**
     * Computes the gradient using the pulled SparseModel and push the gradient to different pss.
     */
    private void computeGradAndPushToPS(byte[] pulledSparseModel, int epochWatermark) {
        PulledModelM pulledModelM = MessageUtils.readFromBytes(pulledSparseModel, 0);
        Preconditions.checkState(
                getRuntimeContext().getIndexOfThisSubtask() == pulledModelM.workerId);
        int modelId = pulledModelM.modelId;
        // if (workerId == 0) {
        //    LOG.error(
        //        "[Worker-{}][iteration-{}] Processing pulled-result from servers, with {} nnzs.",
        //        workerId,
        //        epochWatermark,
        //        pulledModelM.pulledValues.values.length);
        // }

        double[] pulledModelValues = pulledModelM.pulledValues.values;
        long modelDim = psAgent.partitioners.get(modelId).dim;
        SparseLongDoubleVector coefficient =
                new SparseLongDoubleVector(modelDim, sortedBatchIndices, pulledModelValues);

        SparseLongDoubleVector cumGradients =
                new SparseLongDoubleVector(
                        coefficient.size,
                        coefficient.indices.clone(),
                        new double[coefficient.values.length]);
        double totalLoss = 0;
        double lossWeight = 0;
        for (LabeledLargePointWithWeight dataPoint : batchTrainData) {
            // totalLoss += lossFunc.computeLoss(dataPoint, coefficient);
            lossFunc.computeGradient(dataPoint, coefficient, cumGradients);
            lossWeight += dataPoint.weight;
        }
        long currentTimeInMs = System.currentTimeMillis();
        // LOG.error(
        //        "[Worker-{}][iteration-{}] Sending push-gradient to servers, with {} nnzs,
        // timeCost: {} ms",
        //        workerId,
        //        epochWatermark,
        //        cumGradients.indices.length,
        //        currentTimeInMs - lastIterationTime);
        if (epochWatermark > 1) {
            LOG.error(
                    "[Worker-{}][iteration-{}], timeCost: {} ms",
                    workerId,
                    epochWatermark,
                    currentTimeInMs - lastIterationTime);
        }
        lastIterationTime = currentTimeInMs;
        psAgent.sparsePushGradient(
                modelId,
                new SparseLongDoubleVectorStorage(
                        modelDim, cumGradients.indices, cumGradients.values),
                lossWeight);
    }

    private int getNextModelId() {
        return nextModelId++;
    }

    @Override
    public void onEpochWatermarkIncremented(
            int epochWatermark, Context context, Collector<Tuple2<Integer, byte[]>> collector)
            throws Exception {

        if (trainData == null) {
            trainData = IteratorUtils.toList(trainDataState.get().iterator());
        }
        if (epochWatermark == 0) {

            // TODO: add suport for incremental training.
            // only one worker needs to initialize the model by pushing to server.
            long dim = Bits.getLong(feedback, 0);
            int modelId = getNextModelId();
            psAgent.addPartitioner(modelId, new RangeModelPartitioner(dim, numPss, modelId));
            // LOG.error(
            //        "[Worker-{}][iteration-{}] Sending psf-zeros to servers, model dimension is
            // {}}",
            //        workerId,
            //        epochWatermark,
            //        dim);
            if (workerId == 0) {
                psAgent.zeros(modelId, dim);
            }
        } else {
            // When receiving the pulled sparse model.
            computeGradAndPushToPS(feedback, epochWatermark);
        }
        // Compute the sparse model indices and push to ps.
        sampleDataAndSendPullRequest(epochWatermark);
    }

    @Override
    public void onIterationTerminated(
            Context context, Collector<Tuple2<Integer, byte[]>> collector) {
        trainDataState.clear();
    }

    @Override
    public void processElement1(StreamRecord<LabeledLargePointWithWeight> streamRecord)
            throws Exception {
        trainDataState.add(streamRecord.getValue());
    }

    @Override
    public void processElement2(StreamRecord<byte[]> streamRecord) {
        feedback = streamRecord.getValue();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        feedbackState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "feedbackArrayState",
                                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        OperatorStateUtils.getUniqueElement(feedbackState, "feedbackArrayState")
                .ifPresent(x -> feedback = x);

        trainDataState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "trainDataState",
                                        TypeInformation.of(LabeledLargePointWithWeight.class)));

        batchTrainDataState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "batchTrainDataState",
                                        TypeInformation.of(LabeledLargePointWithWeight.class)));

        batchTrainData = IteratorUtils.toList(batchTrainDataState.get().iterator());

        batchOffsetState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "nextBatchOffsetState", BasicTypeInfo.INT_TYPE_INFO));
        batchOffSet =
                OperatorStateUtils.getUniqueElement(batchOffsetState, "nextBatchOffsetState")
                        .orElse(0);

        // partitioner state
        partitionersState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<Map<Integer, RangeModelPartitioner>>(
                                        "partitionersState",
                                        new MapTypeInfo<Integer, RangeModelPartitioner>(
                                                Types.INT,
                                                TypeInformation.of(RangeModelPartitioner.class))));

        partitioners =
                OperatorStateUtils.getUniqueElement(partitionersState, "partitionersState")
                        .orElse(new HashMap<>());
        for (Map.Entry<Integer, RangeModelPartitioner> modelIdAndPartitioner :
                partitioners.entrySet()) {
            psAgent.addPartitioner(
                    modelIdAndPartitioner.getKey(), modelIdAndPartitioner.getValue());
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // coefficientState.clear();
        // if (coefficient != null) {
        //    coefficientState.add(coefficient);
        // }

        feedbackState.clear();
        if (feedback != null) {
            feedbackState.add(feedback);
        }

        batchOffsetState.clear();
        batchOffsetState.add(batchOffSet);

        // snapshots the training data.
        batchTrainDataState.clear();
        if (trainData != null) {
            batchTrainDataState.addAll(trainData);
        }
    }
}
