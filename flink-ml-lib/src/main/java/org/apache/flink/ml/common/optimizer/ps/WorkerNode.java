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
import org.apache.flink.iteration.datacache.nonkeyed.ListStateWithCache;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.iteration.typeinfo.IterationRecordSerializer;
import org.apache.flink.ml.common.feature.LabeledLargePointWithWeight;
import org.apache.flink.ml.common.lossfunc.BinaryLogisticLoss;
import org.apache.flink.ml.common.lossfunc.LossFunc;
import org.apache.flink.ml.common.optimizer.PSFtrl.FTRLParams;
import org.apache.flink.ml.common.optimizer.ps.datastorage.DenseLongVectorStorage;
import org.apache.flink.ml.common.optimizer.ps.datastorage.SparseLongDoubleVectorStorage;
import org.apache.flink.ml.common.optimizer.ps.message.MessageUtils;
import org.apache.flink.ml.common.optimizer.ps.message.PulledModelM;
import org.apache.flink.ml.regression.linearregression.LinearRegression;
import org.apache.flink.ml.util.Bits;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The worker. It does the following things:
 *
 * <ul>
 *   <li>Caches the training data from input table.
 *   <li>Samples a mini-batch of training data and compute indices needed.
 *   <li>Pulls model data from servers.
 *   <li>Pushes gradients to servers.
 * </ul>
 */
public class WorkerNode extends AbstractStreamOperator<Tuple2<Integer, byte[]>>
        implements TwoInputStreamOperator<
                        LabeledLargePointWithWeight, byte[], Tuple2<Integer, byte[]>>,
                IterationListener<Tuple2<Integer, byte[]>> {
    protected static final Logger LOG = LoggerFactory.getLogger(WorkerNode.class);

    /** Optimizer-related parameters. */
    private final FTRLParams params;

    /** The loss function to optimize. */
    private final LossFunc lossFunc;

    /** The cached training data. */
    private ListStateWithCache<LabeledLargePointWithWeight> trainDataState;

    // TODO: Add state for numTrainData.
    private long numTrainData = 0;

    // TODO: Support start from given offset.
    private Iterator<LabeledLargePointWithWeight> trainDataIterator;

    /** The start index (offset) of the next mini-batch data for training. */
    private int batchOffSet = 0;

    private ListState<Integer> batchOffsetState;

    private LabeledLargePointWithWeight[] batchTrainData;
    private int numDataInBatch = 0;
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

    long lastIterationTime = -1;

    int iterationId = -1;

    long logStartTime;

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
        this.batchTrainData = new LabeledLargePointWithWeight[localBatchSize];
        this.workerId = getRuntimeContext().getIndexOfThisSubtask();
        this.psAgent = new PSAgent(workerId, output);
        this.lastIterationTime = System.currentTimeMillis();
    }

    /** Samples a batch of training data and push the needed model index to ps. */
    private void sampleDataAndSendPullRequest(int iterationId) throws Exception {
        // also sample a batch of training data and push the needed sparse models to ps.
        // clear the last batch of training data.
        int modelId = 0;
        numDataInBatch = 0;
        long nextOffSet = Math.min(numTrainData, batchOffSet + localBatchSize);
        // TODO: Add support for checkpointing offset of train data.
        if (trainDataIterator == null) {
            trainDataIterator = trainDataState.get().iterator();
        }
        while (batchOffSet < nextOffSet) {
            batchTrainData[numDataInBatch++] = trainDataIterator.next();
            batchOffSet++;
        }
        sortedBatchIndices = getSortedIndicesFromData(batchTrainData, numDataInBatch);
        if (batchOffSet >= numTrainData) {
            batchOffSet = 0;
            trainDataIterator = trainDataState.get().iterator();
        }
        // LOG.error(
        //        "[Worker-{}][iteration-{}] Sending pull-model request to servers, with {} nnzs.",
        //        workerId,
        //        iterationId,
        //        sortedBatchIndices.length);
        psAgent.sparsePullModel(modelId, new DenseLongVectorStorage(sortedBatchIndices));
    }

    private static long[] getSortedIndicesFromData(
            LabeledLargePointWithWeight[] dataPoints, int numDataInBatch) {
        LongOpenHashSet indices = new LongOpenHashSet(); // flame graph from 12% to 9%.
        for (int i = 0; i < numDataInBatch; i++) {
            LabeledLargePointWithWeight dataPoint = dataPoints[i];
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
    private void computeGradAndPushToPS(byte[] pulledSparseModel, int iterationId) {
        PulledModelM pulledModelM = MessageUtils.readFromBytes(pulledSparseModel, 0);
        Preconditions.checkState(
                getRuntimeContext().getIndexOfThisSubtask() == pulledModelM.workerId);
        int modelId = pulledModelM.modelId;
        double[] pulledModelValues = pulledModelM.pulledValues.values;
        long modelDim = psAgent.partitioners.get(modelId).dim;
        if (sortedBatchIndices == null) {
            sortedBatchIndices = getSortedIndicesFromData(batchTrainData, numDataInBatch);
        }

        Long2DoubleOpenHashMap coefficient = new Long2DoubleOpenHashMap(sortedBatchIndices.length);
        for (int i = 0; i < sortedBatchIndices.length; i++) {
            coefficient.put(sortedBatchIndices[i], pulledModelValues[i]);
        }
        Long2DoubleOpenHashMap cumGradients = new Long2DoubleOpenHashMap(sortedBatchIndices.length);

        double totalLoss = 0;
        double lossWeight = 0;
        for (int i = 0; i < numDataInBatch; i++) {
            LabeledLargePointWithWeight dataPoint = batchTrainData[i];
            double dot = BinaryLogisticLoss.dot(dataPoint.features, coefficient);
            totalLoss += lossFunc.computeLossWithDot(dataPoint, coefficient, dot);
            lossFunc.computeGradientWithDot(dataPoint, coefficient, cumGradients, dot);
            lossWeight += dataPoint.weight;
        }
        long currentTimeInMs = System.currentTimeMillis();
        LOG.error(
                "[Worker-{}][iteration-{}] push gradient to servers, with {} nnzs, local loss: {}, timeCost: {} ms.",
                workerId,
                iterationId,
                sortedBatchIndices.length,
                totalLoss,
                currentTimeInMs - lastIterationTime);
        lastIterationTime = currentTimeInMs;
        double[] cumGradientValues = new double[sortedBatchIndices.length];
        for (int i = 0; i < sortedBatchIndices.length; i++) {
            cumGradientValues[i] = cumGradients.get(sortedBatchIndices[i]);
        }
        psAgent.sparsePushGradient(
                modelId,
                new SparseLongDoubleVectorStorage(modelDim, sortedBatchIndices, cumGradientValues),
                lossWeight);
    }

    private int getNextModelId() {
        return nextModelId++;
    }

    @Override
    public void onEpochWatermarkIncremented(
            int epochWatermark, Context context, Collector<Tuple2<Integer, byte[]>> collector)
            throws Exception {
        // LOG.error("[watermark] Worker {} incremented to watermark {}", workerId, epochWatermark);
        if (epochWatermark == 0) {
            iterationId = 0;
            // TODO: add support for incremental training.
            long dim = Bits.getLong(feedback, 0);
            int modelId = getNextModelId();
            psAgent.addPartitioner(modelId, new RangeModelPartitioner(dim, numPss, modelId));

            // All workers send the initialization instruction to servers for simplicity.
            psAgent.zeros(modelId, dim);
            // LOG.error(
            //        "[Worker-{}][iteration-{}] Sending psf-zeros to servers, model dimension is
            // {}",
            //        workerId,
            //        epochWatermark,
            //        dim);
            sampleDataAndSendPullRequest(epochWatermark);
        }
    }

    @Override
    public void onIterationTerminated(
            Context context, Collector<Tuple2<Integer, byte[]>> collector) {
        trainDataState.clear();
        LOG.error("Whole iteration takes: " + (System.currentTimeMillis() - logStartTime));
    }

    @Override
    public void processElement1(StreamRecord<LabeledLargePointWithWeight> streamRecord)
            throws Exception {
        trainDataState.add(streamRecord.getValue());
        numTrainData++;
    }

    @Override
    public void processElement2(StreamRecord<byte[]> streamRecord) throws Exception {
        feedback = streamRecord.getValue();
        if (iterationId >= 0) {
            if (iterationId == 0) {
                logStartTime = System.currentTimeMillis();
            }
            computeGradAndPushToPS(feedback, iterationId);
            iterationId++;
            if (iterationId < params.maxIter) {
                sampleDataAndSendPullRequest(iterationId);
            }
        }
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

        // TODO: fix the incorrect stream config.
        trainDataState =
                new ListStateWithCache<>(
                        ((IterationRecordSerializer)
                                        getOperatorConfig()
                                                .getTypeSerializerIn(
                                                        0, getClass().getClassLoader()))
                                .getInnerSerializer(),
                        getContainingTask(),
                        getRuntimeContext(),
                        context,
                        config.getOperatorID());

        batchTrainDataState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "batchTrainDataState",
                                        TypeInformation.of(LabeledLargePointWithWeight.class)));

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
        feedbackState.clear();
        if (feedback != null) {
            feedbackState.add(feedback);
        }

        batchOffsetState.clear();
        batchOffsetState.add(batchOffSet);
        trainDataState.snapshotState(context);
    }
}
