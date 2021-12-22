/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.classification.logisticregression.async;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.classification.logisticregression.LogisticGradient;
import org.apache.flink.ml.classification.logisticregression.LogisticRegression;
import org.apache.flink.ml.classification.logisticregression.LogisticRegressionModel;
import org.apache.flink.ml.classification.logisticregression.LogisticRegressionModelData;
import org.apache.flink.ml.classification.logisticregression.LogisticRegressionParams;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.feature.LabeledPointWithWeight;
import org.apache.flink.ml.common.objkeeper.ObjectKeeper;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.SparseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.param.HasUniqueID;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.IteratorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * An Estimator which implements the logistic regression algorithm.
 *
 * <p>See https://en.wikipedia.org/wiki/Logistic_regression.
 */
public class LogisticRegressionAsyncPS
        implements Estimator<LogisticRegressionAsyncPS, LogisticRegressionModel>,
                LogisticRegressionParams<LogisticRegressionAsyncPS>,
                HasUniqueID<LogisticRegressionAsyncPS> {

    private Map<Param<?>, Object> paramMap = new HashMap<>();

    public LogisticRegressionAsyncPS() {
        ParamUtils.initializeMapWithDefaultValues(this.paramMap, this);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static LogisticRegression load(StreamExecutionEnvironment env, String path)
            throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    @SuppressWarnings({"rawTypes", "ConstantConditions"})
    public LogisticRegressionModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        String classificationType = getMultiClass();
        Preconditions.checkArgument(
                "auto".equals(classificationType) || "binomial".equals(classificationType),
                "Multinomial classification is not supported yet. Supported options: [auto, binomial].");
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<LabeledPointWithWeight> trainData =
                tEnv.toDataStream(inputs[0])
                        .map(
                                dataPoint -> {
                                    Double weight =
                                            getWeightCol() == null
                                                    ? new Double(1.0)
                                                    : (Double) dataPoint.getField(getWeightCol());
                                    Double label = (Double) dataPoint.getField(getLabelCol());
                                    boolean isBinomial =
                                            Double.compare(0., label) == 0
                                                    || Double.compare(1., label) == 0;
                                    if (!isBinomial) {
                                        throw new RuntimeException(
                                                "Multinomial classification is not supported yet. Supported options: [auto, binomial]"
                                                        + ".");
                                    }
                                    Vector features = (Vector) dataPoint.getField(getFeaturesCol());
                                    return new LabeledPointWithWeight(features, label, weight);
                                });
        final int numPss = 3;
        //// only one model.
        // DataStream<Integer> modelDim = DataStreamUtils.reduce(trainData.map(x ->
        // x.features.size()),
        //    new ReduceFunction <Integer>() {
        //        @Override
        //        public Integer reduce(Integer value1, Integer value2) throws Exception {
        //            return Math.max(value1, value2);
        //        }
        //    });
        // modelDim.broadcast()

        // DataStream<Double> modelDim =
        //        trainData
        //                .transform(
        //                        "genInitModelData",
        //                        PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
        //                        new GenInitModelData())
        //                .setParallelism(1);
        // DataStream<double[]> partitionedModelData = initModelData.map()

        DataStream<LogisticRegressionModelData> modelData = train(trainData, numPss);
        LogisticRegressionModel model =
                new LogisticRegressionModel().setModelData(tEnv.fromDataStream(modelData));
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    /**
     * Generates initialized model data. Note that the parallelism of model data is same as the
     * input train data, not one.
     */
    private static class GenInitModelData extends AbstractStreamOperator<double[]>
            implements OneInputStreamOperator<LabeledPointWithWeight, double[]>, BoundedOneInput {

        private int dim = 0;

        private ListState<Integer> dimState;

        @Override
        public void endInput() {
            output.collect(new StreamRecord<>(new double[dim]));
        }

        @Override
        public void processElement(StreamRecord<LabeledPointWithWeight> streamRecord) {
            if (dim == 0) {
                dim = streamRecord.getValue().getFeatures().size();
            } else {
                if (dim != streamRecord.getValue().getFeatures().size()) {
                    throw new RuntimeException(
                            "The training data should all have same dimensions.");
                }
            }
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            dimState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "dimState", BasicTypeInfo.INT_TYPE_INFO));
            dim = OperatorStateUtils.getUniqueElement(dimState, "dimState").orElse(0);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            dimState.clear();
            dimState.add(dim);
        }
    }

    /**
     * Does machine learning training on the input data with the initialized model data.
     *
     * @param trainData The training data.
     * @param numPSs Number of parameter servers.
     * @return The trained model data.
     */
    private DataStream<LogisticRegressionModelData> train(
            DataStream<LabeledPointWithWeight> trainData, int numPSs) {
        LogisticGradient logisticGradient = new LogisticGradient(getReg());
        DataStream<Double> initLoss =
                DataStreamUtils.mapPartition(
                        trainData,
                        new MapPartitionFunction<LabeledPointWithWeight, Double>() {
                            @Override
                            public void mapPartition(
                                    Iterable<LabeledPointWithWeight> values, Collector<Double> out)
                                    throws Exception {
                                out.collect(1.0);
                            }
                        });

        DataStream<Integer> modelDim =
                DataStreamUtils.reduce(
                        trainData.map(x -> x.getFeatures().size()),
                        new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return Math.max(value1, value2);
                            }
                        });
        ModelPartitioner modelPartitioner = new ModelPartitioner(numPSs);
        DataStream<double[]> initModel =
                modelDim.broadcast()
                        .map(new GenPartitionedModel(modelPartitioner))
                        .setParallelism(numPSs);

        final String modelHandle = ObjectKeeper.getNewKey(getUniqueID());
        final String batchDataHandle = ObjectKeeper.getNewKey(getUniqueID());
        DataStreamList resultList =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(initLoss),
                        ReplayableDataStreamList.notReplay(trainData, initModel),
                        IterationConfig.newBuilder().build(),
                        new AsyncTrainIterationBody(
                                logisticGradient,
                                getGlobalBatchSize(),
                                getLearningRate(),
                                getMaxIter(),
                                getTol(),
                                numPSs,
                                batchDataHandle,
                                getUniqueID().toHexString(),
                                modelHandle,
                                modelPartitioner));
        return resultList.get(0);
    }

    private static class GenPartitionedModel extends RichMapFunction<Integer, double[]> {
        private final ModelPartitioner modelPartitioner;

        public GenPartitionedModel(ModelPartitioner modelPartitioner) {
            this.modelPartitioner = modelPartitioner;
        }

        @Override
        public double[] map(Integer modeDim) throws Exception {
            int taskId = getRuntimeContext().getIndexOfThisSubtask();
            int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
            Preconditions.checkArgument(numTasks == modelPartitioner.numPSs);
            int numDimsHandled = modelPartitioner.getPsCapacity(modeDim, taskId);
            return new double[numDimsHandled];
        }
    }

    /** The iteration implementation for training process. */
    private static class AsyncTrainIterationBody implements IterationBody {

        private final LogisticGradient logisticGradient;

        private final int globalBatchSize;

        private final double learningRate;

        private final int maxIter;

        private final double tol;

        private final int numPSs;

        private final String batchDataHandle;

        private final String uniqueKey;

        private final String modelHandle;

        private final ModelPartitioner modelPartitioner;

        public AsyncTrainIterationBody(
                LogisticGradient logisticGradient,
                int globalBatchSize,
                double learningRate,
                int maxIter,
                double tol,
                int numPSs,
                String batchDataHandle,
                String uniqueKey,
                String modelHandle,
                ModelPartitioner modelPartitioner) {
            this.logisticGradient = logisticGradient;
            this.globalBatchSize = globalBatchSize;
            this.learningRate = learningRate;
            this.maxIter = maxIter;
            this.tol = tol;
            this.numPSs = numPSs;
            this.batchDataHandle = batchDataHandle;
            this.uniqueKey = uniqueKey;
            this.modelHandle = modelHandle;
            this.modelPartitioner = modelPartitioner;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Double> lossStream = variableStreams.get(0);
            DataStream<LabeledPointWithWeight> trainData = dataStreams.get(0);
            DataStream<double[]> shardedModel = dataStreams.get(1);
            final OutputTag<Tuple2<double[], Integer>> modelDataOutputTag =
                    new OutputTag<Tuple2<double[], Integer>>("MODEL_OUTPUT") {};

            // workerId, indices
            DataStream<Tuple2<Integer, int[]>> workerIdAndPullIndices =
                    trainData
                            .connect(lossStream)
                            .transform(
                                    "cacheDataAndGetPullIndices",
                                    new TupleTypeInfo<>(
                                            BasicTypeInfo.INT_TYPE_INFO,
                                            PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO),
                                    new CacheDataAndGetPullIndices(
                                            globalBatchSize, batchDataHandle, maxIter));
            workerIdAndPullIndices
                    .getTransformation()
                    .setCoLocationGroupKey(uniqueKey + "_workerOperator");

            // workerId, psId, indices
            DataStream<Tuple3<Integer, Integer, int[]>> workerIdAndPsIdAndPullIndices =
                    workerIdAndPullIndices
                            .flatMap(new SplitPullRequest(modelPartitioner))
                            .partitionCustom(
                                    new Partitioner<Integer>() {
                                        @Override
                                        public int partition(Integer key, int numPartitions) {
                                            return key % numPartitions;
                                        }
                                    },
                                    x -> x.f1);

            // workerId, psId, values
            DataStream<Tuple3<Integer, Integer, double[]>> workerIdAndPsIdAndPulledValues =
                    workerIdAndPsIdAndPullIndices
                            .connect(shardedModel)
                            .transform(
                                    "cacheModelAndPushIndicesToWorkers",
                                    new TupleTypeInfo<>(
                                            Types.INT,
                                            Types.INT,
                                            PrimitiveArrayTypeInfo
                                                    .DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO),
                                    new CacheModelAndPushValuesToWorkers(
                                            modelHandle, modelPartitioner));
            workerIdAndPsIdAndPulledValues
                    .getTransformation()
                    .setCoLocationGroupKey(uniqueKey + "_psOperator");

            // workerId, pulledModelValues
            DataStream<Tuple2<Integer, double[]>> workerIdAndPulledValues =
                    workerIdAndPsIdAndPulledValues
                            .partitionCustom(
                                    new Partitioner<Integer>() {
                                        @Override
                                        public int partition(Integer key, int numPartitions) {
                                            return key % numPartitions;
                                        }
                                    },
                                    x -> x.f0)
                            .transform(
                                    "concatePulledModel",
                                    new TupleTypeInfo<>(
                                            Types.INT,
                                            PrimitiveArrayTypeInfo
                                                    .DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO),
                                    new ConcatePulledModel(
                                            numPSs, batchDataHandle, modelPartitioner));
            workerIdAndPulledValues
                    .getTransformation()
                    .setCoLocationGroupKey(uniqueKey + "_workerOperator");

            // workerId, gradients(indices, values)
            DataStream<Tuple3<Integer, int[], double[]>> workerIdAndGradients =
                    workerIdAndPulledValues.transform(
                            "computeGradient",
                            new TupleTypeInfo<>(
                                    Types.INT,
                                    Types.INT,
                                    PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO),
                            new ComputeGradient(batchDataHandle, new LogisticGradient(0)));

            // push gradients to model storage

            DataStream<Tuple4<Integer, Integer, int[], double[]>> workerIdAndPsIdAndGradients =
                    workerIdAndGradients
                            .flatMap(new SplitGradients(modelPartitioner))
                            .returns(
                                    new TupleTypeInfo<>(
                                            Types.INT,
                                            Types.INT,
                                            PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO,
                                            PrimitiveArrayTypeInfo
                                                    .DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));

            // workerId, psId,
            SingleOutputStreamOperator<Tuple2<Integer, Integer>> epochEndSignal =
                    workerIdAndPsIdAndGradients
                            .partitionCustom(
                                    new Partitioner<Integer>() {
                                        @Override
                                        public int partition(Integer key, int numPartitions) {
                                            return key % numPartitions;
                                        }
                                    },
                                    x -> x.f1)
                            .transform(
                                    "updateModel",
                                    new TupleTypeInfo(Types.INT, Types.INT),
                                    new UpdateModelAndEmitEndSignal(
                                            modelHandle, learningRate, modelDataOutputTag));

            epochEndSignal.getTransformation().setCoLocationGroupKey(uniqueKey + "_psOperator");

            DataStream<Integer> feedback =
                    epochEndSignal
                            .partitionCustom(
                                    new Partitioner<Integer>() {
                                        @Override
                                        public int partition(Integer key, int numPartitions) {
                                            return key % numPartitions;
                                        }
                                    },
                                    workerIdAndPsId -> workerIdAndPsId.f0)
                            .transform(
                                    "syncOnDifferentPssForEachWorker",
                                    Types.INT,
                                    new SyncPS(numPSs));
            return new IterationBodyResult(
                    DataStreamList.of(feedback),
                    DataStreamList.of(epochEndSignal.getSideOutput(modelDataOutputTag)),
                    null);
        }
    }

    private static class SyncPS extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Tuple2<Integer, Integer>, Integer> {
        private final int numPss;
        private int numSignalsReceived = 0;
        private ListState<Integer> numSignalsReceivedState;

        public SyncPS(int numPss) {
            this.numPss = numPss;
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, Integer>> streamRecord)
                throws Exception {
            numSignalsReceived++;
            if (numSignalsReceived == numPss) {
                output.collect(new StreamRecord<>(1));
                numSignalsReceived = 0;
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            numSignalsReceivedState.clear();
            numSignalsReceivedState.add(numSignalsReceived);
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            numSignalsReceivedState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "numSignalsReceivedState", Types.INT));
            numSignalsReceived =
                    OperatorStateUtils.getUniqueElement(
                                    numSignalsReceivedState, "numSignalsReceivedState")
                            .orElse(0);
        }
    }

    private static class UpdateModelAndEmitEndSignal
            extends AbstractStreamOperator<Tuple2<Integer, Integer>>
            implements OneInputStreamOperator<
                            Tuple4<Integer, Integer, int[], double[]>, Tuple2<Integer, Integer>>,
                    IterationListener<Tuple2<double[], Integer>> {

        private final String modelHandle;
        private final double learningRate;
        private int psId;
        private final OutputTag<Tuple2<double[], Integer>> modelDataOutputTag;

        public UpdateModelAndEmitEndSignal(
                String modelHandle,
                double learningRate,
                OutputTag<Tuple2<double[], Integer>> modelDataOutputTag) {
            this.modelHandle = modelHandle;
            this.learningRate = learningRate;
            this.modelDataOutputTag = modelDataOutputTag;
        }

        @Override
        public void open() {
            psId = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void processElement(
                StreamRecord<Tuple4<Integer, Integer, int[], double[]>> streamRecord)
                throws Exception {
            Tuple4<Integer, Integer, int[], double[]> tuple = streamRecord.getValue();
            int workerId = tuple.f0;
            int psId = tuple.f1;
            int[] indices = tuple.f2;
            double[] values = tuple.f3;
            double[] modelPiece = ObjectKeeper.get(Tuple2.of(modelHandle, psId));
            BLAS.axpy(
                    -learningRate,
                    new SparseVector(modelPiece.length, indices, values),
                    new DenseVector(modelPiece));
            output.collect(new StreamRecord<>(Tuple2.of(workerId, psId)));
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<Tuple2<double[], Integer>> collector)
                throws Exception {}

        @Override
        public void onIterationTerminated(
                Context context, Collector<Tuple2<double[], Integer>> collector) throws Exception {
            double[] modelPiece = ObjectKeeper.get(Tuple2.of(modelHandle, psId));
            context.output(modelDataOutputTag, Tuple2.of(modelPiece, psId));
        }

        @Override
        public void close() throws Exception {
            super.close();
            ObjectKeeper.remove(
                    Tuple2.of(modelHandle, getRuntimeContext().getIndexOfThisSubtask()));
        }
    }

    private static class SplitGradients
            implements FlatMapFunction<
                    Tuple3<Integer, int[], double[]>, Tuple4<Integer, Integer, int[], double[]>> {

        private final ModelPartitioner modelPartitioner;

        public SplitGradients(ModelPartitioner modelPartitioner) {
            this.modelPartitioner = modelPartitioner;
        }

        @Override
        public void flatMap(
                Tuple3<Integer, int[], double[]> value,
                Collector<Tuple4<Integer, Integer, int[], double[]>> out)
                throws Exception {
            int numPss = modelPartitioner.numPSs;
            List<Integer>[] indices = new ArrayList[numPss];
            List<Double>[] values = new ArrayList[numPss];
            for (int i = 0; i < numPss; i++) {
                indices[i] = new ArrayList<>();
                values[i] = new ArrayList<>();
            }
            int workerId = value.f0;
            int[] gradientIndices = value.f1;
            double[] gradientValues = value.f2;
            for (int i = 0; i < gradientIndices.length; i++) {
                int psId = modelPartitioner.getPsId(gradientIndices[i]);
                indices[psId].add(gradientIndices[i]);
                values[psId].add(gradientValues[i]);
            }
            for (int psId = 0; psId < numPss; psId++) {
                out.collect(
                        Tuple4.of(
                                workerId,
                                psId,
                                indices[psId].stream().mapToInt(Integer::intValue).toArray(),
                                values[psId].stream().mapToDouble(Double::doubleValue).toArray()));
            }
        }
    }

    private static class ConcatePulledModel
            extends AbstractStreamOperator<Tuple2<Integer, double[]>>
            implements OneInputStreamOperator<
                    Tuple3<Integer, Integer, double[]>, Tuple2<Integer, double[]>> {

        private List<Tuple3<Integer, Integer, double[]>> receivedPulledValues = new ArrayList<>();
        private final int numPss;
        private final String batchDataHandle;
        private final ModelPartitioner modelPartitioner;

        public ConcatePulledModel(
                int numPss, String batchDataHandle, ModelPartitioner modelPartitioner) {
            this.numPss = numPss;
            this.batchDataHandle = batchDataHandle;
            this.modelPartitioner = modelPartitioner;
        }

        @Override
        public void processElement(StreamRecord<Tuple3<Integer, Integer, double[]>> streamRecord)
                throws Exception {
            receivedPulledValues.add(streamRecord.getValue());
            // check if we have received all model pieces
            if (receivedPulledValues.size() == numPss) {
                Tuple2<List<LabeledPointWithWeight>, int[]> dataAndNeedIndices =
                        ObjectKeeper.get(
                                Tuple2.of(
                                        batchDataHandle,
                                        getRuntimeContext().getIndexOfThisSubtask()));
                double[][] splitedPulledValues = new double[numPss][];
                for (Tuple3<Integer, Integer, double[]> workerIdAndPsIdAndSplitedPulledValues :
                        receivedPulledValues) {
                    splitedPulledValues[workerIdAndPsIdAndSplitedPulledValues.f1] =
                            workerIdAndPsIdAndSplitedPulledValues.f2;
                }
                int[] neededIndices = dataAndNeedIndices.f1;
                int[] indexOnEachPs = new int[numPss];
                double[] pulledValues = new double[neededIndices.length];
                for (int i = 0; i < neededIndices.length; i++) {
                    int psId = modelPartitioner.getPsId(neededIndices[i]);
                    pulledValues[i] = splitedPulledValues[psId][indexOnEachPs[psId]];
                    indexOnEachPs[psId]++;
                }
                output.collect(
                        new StreamRecord<>(
                                Tuple2.of(
                                        getRuntimeContext().getIndexOfThisSubtask(),
                                        pulledValues)));
            }
        }
    }

    private static class ComputeGradient
            extends AbstractStreamOperator<Tuple3<Integer, int[], double[]>>
            implements OneInputStreamOperator<
                            Tuple2<Integer, double[]>, Tuple3<Integer, int[], double[]>>,
                    IterationListener<Object> {

        private final String batchDataHandle;
        private final LogisticGradient logisticGradient;

        public ComputeGradient(String batchDataHandle, LogisticGradient logisticGradient) {
            this.batchDataHandle = batchDataHandle;
            this.logisticGradient = logisticGradient;
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<Object> collector) {}

        @Override
        public void onIterationTerminated(Context context, Collector<Object> collector) {}

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, double[]>> streamRecord)
                throws Exception {
            Tuple2<List<LabeledPointWithWeight>, int[]> dataAndIndices =
                    ObjectKeeper.get(
                            Tuple2.of(
                                    batchDataHandle, getRuntimeContext().getIndexOfThisSubtask()));
            Preconditions.checkNotNull(dataAndIndices);
            double[] pulledValues = streamRecord.getValue().f1;
            if (dataAndIndices.f0.size() > 0) {
                int modelDim = dataAndIndices.f0.get(0).getFeatures().size();
                SparseVector model = new SparseVector(modelDim, dataAndIndices.f1, pulledValues);
                // TODO: make it a sparse vector
                DenseVector gradient = new DenseVector(modelDim);
                logisticGradient.computeGradient(
                        dataAndIndices.f0, model, new DenseVector(modelDim));
                Tuple2<Double, Double> weightSumAndLossSum =
                        logisticGradient.computeLoss(dataAndIndices.f0, model);
                System.out.println(
                        "The loss is: " + weightSumAndLossSum.f1 / weightSumAndLossSum.f0);
                List<Integer> nonZeroIndices = new ArrayList<>();
                List<Double> nonZeroValues = new ArrayList<>();
                for (int i = 0; i < gradient.size(); i++) {
                    // TODO: could do quantization here.
                    if (Math.abs(gradient.get(i)) < 1e-5) {
                        nonZeroIndices.add(i);
                        nonZeroValues.add(gradient.get(i));
                    }
                }
                output.collect(
                        new StreamRecord<>(
                                Tuple3.of(
                                        getRuntimeContext().getIndexOfThisSubtask(),
                                        nonZeroIndices.stream()
                                                .mapToInt(Integer::intValue)
                                                .toArray(),
                                        nonZeroValues.stream()
                                                .mapToDouble(Double::doubleValue)
                                                .toArray())));
            }
        }
    }

    private static class CacheModelAndPushValuesToWorkers
            extends AbstractStreamOperator<Tuple3<Integer, Integer, double[]>>
            implements TwoInputStreamOperator<
                            Tuple3<Integer, Integer, int[]>,
                            double[],
                            Tuple3<Integer, Integer, double[]>>,
                    IterationListener<double[]>,
                    InputSelectable {

        private final String modelHandle;
        private final ModelPartitioner modelPartitioner;
        private double[] modelPiece;
        private ListState<double[]> modelPieceState;
        private InputSelection inputSelection;

        public CacheModelAndPushValuesToWorkers(
                String modelHandle, ModelPartitioner modelPartitioner) {
            this.modelHandle = modelHandle;
            this.modelPartitioner = modelPartitioner;
            inputSelection = InputSelection.SECOND;
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<double[]> collector) {}

        @Override
        public void onIterationTerminated(Context context, Collector<double[]> collector) {}

        @Override
        public void processElement1(StreamRecord<Tuple3<Integer, Integer, int[]>> streamRecord)
                throws Exception {
            Preconditions.checkNotNull(modelPiece);
            Tuple3<Integer, Integer, int[]> workerIdPsIdAndPullIndices = streamRecord.getValue();
            int[] pullIndices = workerIdPsIdAndPullIndices.f2;
            double[] pulledValues = new double[pullIndices.length];
            for (int i = 0; i < pullIndices.length; i++) {
                pulledValues[i] = modelPiece[modelPartitioner.getLocalIndex(pullIndices[i])];
            }
            output.collect(
                    new StreamRecord<>(
                            Tuple3.of(
                                    workerIdPsIdAndPullIndices.f0,
                                    workerIdPsIdAndPullIndices.f1,
                                    pulledValues)));
        }

        // caches the model in static memory. There should be only one element here.
        @Override
        public void processElement2(StreamRecord<double[]> streamRecord) throws Exception {
            modelPiece = streamRecord.getValue();
            ObjectKeeper.put(
                    Tuple2.of(modelHandle, getRuntimeContext().getIndexOfThisSubtask()),
                    modelPiece);
            inputSelection = InputSelection.FIRST;
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            modelPieceState.clear();
            modelPieceState.add(modelPiece);
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            modelPieceState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "modelPieceState",
                                            PrimitiveArrayTypeInfo
                                                    .DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
            modelPiece =
                    OperatorStateUtils.getUniqueElement(modelPieceState, "modelPieceState")
                            .orElse(null);
            if (modelPiece != null) {
                ObjectKeeper.put(
                        Tuple2.of(modelHandle, getRuntimeContext().getIndexOfThisSubtask()),
                        modelPiece);
                inputSelection = InputSelection.FIRST;
            }
        }

        @Override
        public InputSelection nextSelection() {
            return inputSelection;
        }
    }

    private static class SplitPullRequest
            extends RichFlatMapFunction<Tuple2<Integer, int[]>, Tuple3<Integer, Integer, int[]>> {
        private final ModelPartitioner modelPartitioner;

        public SplitPullRequest(ModelPartitioner modelPartitioner) {
            this.modelPartitioner = modelPartitioner;
        }

        @Override
        public void flatMap(
                Tuple2<Integer, int[]> value, Collector<Tuple3<Integer, Integer, int[]>> out)
                throws Exception {
            int numPss = modelPartitioner.numPSs;
            int workerId = value.f0;
            int[] toPullIndices = value.f1;
            List<Integer>[] splittedIndices = new ArrayList[numPss];
            for (int i = 0; i < splittedIndices.length; i++) {
                splittedIndices[i] = new ArrayList<>(value.f1.length / numPss);
            }
            for (int index : toPullIndices) {
                splittedIndices[modelPartitioner.getPsId(index)].add(index);
            }
            for (int psId = 0; psId < splittedIndices.length; psId++) {
                out.collect(
                        Tuple3.of(
                                workerId,
                                psId,
                                splittedIndices[psId].stream()
                                        .mapToInt(Integer::intValue)
                                        .toArray()));
            }
        }
    }

    private static class CacheDataAndGetPullIndices
            extends AbstractStreamOperator<Tuple2<Integer, int[]>>
            implements TwoInputStreamOperator<
                            LabeledPointWithWeight, Double, Tuple2<Integer, int[]>>,
                    IterationListener<int[]>,
                    InputSelectable {

        private final int globalBatchSize;
        private final String batchDataHandle;

        private ListState<LabeledPointWithWeight> trainDataState;
        private List<LabeledPointWithWeight> trainData;

        private final int maxIter;

        private int localBatchSize;

        private Random random = new Random(2021);

        private List<LabeledPointWithWeight> miniBatchData;

        private int[] neededModelIndices;

        private int globalEpochId = 0;

        private static final int staleness = 2;

        private int localEpochId = 0;
        private ListState<Integer> localEpochIdState;

        private InputSelection inputSelection;

        public CacheDataAndGetPullIndices(
                int globalBatchSize, String batchDataHandle, int maxIter) {
            this.globalBatchSize = globalBatchSize;
            this.batchDataHandle = batchDataHandle;
            this.maxIter = maxIter;
            inputSelection = InputSelection.FIRST;
        }

        @Override
        public void open() {
            int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
            int taskId = getRuntimeContext().getIndexOfThisSubtask();
            localBatchSize = globalBatchSize / numTasks;
            if (globalBatchSize % numTasks > taskId) {
                localBatchSize++;
            }
            this.miniBatchData = new ArrayList<>(localBatchSize);
        }

        private List<LabeledPointWithWeight> getMiniBatchData(
                List<LabeledPointWithWeight> fullBatchData, int batchSize) {
            miniBatchData.clear();
            for (int i = 0; i < batchSize; i++) {
                miniBatchData.add(fullBatchData.get(random.nextInt(fullBatchData.size())));
            }
            return miniBatchData;
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<int[]> collector) {
            globalEpochId = epochWatermark;
        }

        @Override
        public void onIterationTerminated(Context context, Collector<int[]> collector) {}

        @Override
        public void processElement1(StreamRecord<LabeledPointWithWeight> streamRecord)
                throws Exception {
            trainDataState.add(streamRecord.getValue());
            inputSelection = InputSelection.SECOND;
        }

        /**
         * The feedback here is loss. Only we receive one loss from the feedback, we know that this
         * worker finished one epoch.
         *
         * @param streamRecord
         * @throws Exception
         */
        @Override
        public void processElement2(StreamRecord<Double> streamRecord) throws Exception {
            try {
                if (trainData == null) {
                    trainData = IteratorUtils.toList(trainDataState.get().iterator());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            miniBatchData = getMiniBatchData(trainData, localBatchSize);
            neededModelIndices = getPullIndices(miniBatchData);
            // put miniBatchData in static memory.
            ObjectKeeper.put(
                    Tuple2.of(batchDataHandle, getRuntimeContext().getIndexOfThisSubtask()),
                    Tuple2.of(miniBatchData, neededModelIndices));
            System.out.println(
                    "WorkerId: "
                            + getRuntimeContext().getIndexOfThisSubtask()
                            + ", localEpochId: "
                            + localEpochId
                            + ", globalEpochId: "
                            + globalEpochId
                            + ", loss: "
                            + streamRecord.getValue());
            while (localEpochId - globalEpochId > staleness) {
                // wait for other workers.
                System.out.println(
                        "WorkerId: "
                                + getRuntimeContext().getIndexOfThisSubtask()
                                + ", localEpochId: "
                                + localEpochId
                                + ", globalEpochId: "
                                + globalEpochId
                                + ", loss: "
                                + streamRecord.getValue()
                                + ", waiting for other workers...");
                Thread.sleep(100);
            }
            localEpochId++;
            System.out.println(
                    "WorkerId: "
                            + getRuntimeContext().getIndexOfThisSubtask()
                            + ", proceeds to next iteration: "
                            + localEpochId);
            if (localEpochId < maxIter) {
                output.collect(
                        new StreamRecord<>(
                                Tuple2.of(
                                        getRuntimeContext().getIndexOfThisSubtask(),
                                        neededModelIndices)));
            }
        }

        private int[] getPullIndices(List<LabeledPointWithWeight> dataPoints) {
            HashSet<Integer> nonZeroIndices = new HashSet<>();
            for (LabeledPointWithWeight dataPoint : dataPoints) {
                Vector features = dataPoint.getFeatures();
                if (features instanceof DenseVector) {
                    int dim = features.size();
                    for (int i = 0; i < dim; i++) {
                        nonZeroIndices.add(i);
                    }
                    break;
                } else {
                    int[] dataIndices = ((SparseVector) features).indices;
                    for (int index : dataIndices) {
                        nonZeroIndices.add(index);
                    }
                }
            }
            int[] pullOrderedIndices =
                    nonZeroIndices.stream().mapToInt(Integer::intValue).toArray();
            Arrays.sort(pullOrderedIndices);
            return pullOrderedIndices;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            trainDataState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "trainDataState",
                                            TypeInformation.of(LabeledPointWithWeight.class)));

            localEpochIdState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<Integer>(
                                            "localEpochIdState", BasicTypeInfo.INT_TYPE_INFO));
            localEpochId =
                    OperatorStateUtils.getUniqueElement(localEpochIdState, "localEpochIdState")
                            .orElse(0);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {}

        @Override
        public void close() throws Exception {
            super.close();
            ObjectKeeper.remove(
                    Tuple2.of(batchDataHandle, getRuntimeContext().getIndexOfThisSubtask()));
        }

        @Override
        public InputSelection nextSelection() {
            return inputSelection;
        }
    }
}
