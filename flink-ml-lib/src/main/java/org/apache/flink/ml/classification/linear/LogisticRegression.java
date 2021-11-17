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

package org.apache.flink.ml.classification.linear;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.IterationConfig.OperatorLifeCycle;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.common.feature.LabeledPointWithWeight;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIterOrTol;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
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
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * This class implements methods to train a logistic regression model. For details, see
 * https://en.wikipedia.org/wiki/Logistic_regression.
 */
public class LogisticRegression
        implements Estimator<LogisticRegression, LogisticRegressionModel>,
                LogisticRegressionParams<LogisticRegression> {

    private Map<Param<?>, Object> paramMap = new HashMap<>();

    private static final OutputTag<LogisticRegressionModelData> MODEL_OUTPUT =
            new OutputTag<LogisticRegressionModelData>("MODEL_OUTPUT") {};

    private static final OutputTag<double[]> LOSS_OUTPUT =
            new OutputTag<double[]>("LOSS_OUTPUT") {};

    public LogisticRegression() {
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
    @SuppressWarnings("rawTypes")
    public LogisticRegressionModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        Preconditions.checkArgument(
                "auto".equals(getMultiClass()) || "bionomial".equals(getMultiClass()),
                "Currently we only support binary classification.");
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
                                    assert label != null;
                                    assert weight != null;
                                    boolean isBionomialLabel =
                                            Double.compare(0., label) == 0
                                                    || Double.compare(1., label) == 0;
                                    if (!isBionomialLabel) {
                                        throw new RuntimeException(
                                                "Currently we only support binary classification.");
                                    }
                                    DenseVector features =
                                            (DenseVector) dataPoint.getField(getFeaturesCol());
                                    return new LabeledPointWithWeight(features, label, weight);
                                });
        DataStream<double[]> initModel =
                trainData.transform(
                        "genInitModel",
                        PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
                        new GenInitModel());

        DataStream<LogisticRegressionModelData> modelData = train(trainData, initModel);
        LogisticRegressionModel model =
                new LogisticRegressionModel().setModelData(tEnv.fromDataStream(modelData));
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    /**
     * Generates initialized model. Note that the parallelism of model is same as the input train
     * data, not one.
     */
    private static class GenInitModel extends AbstractStreamOperator<double[]>
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
                dim = streamRecord.getValue().features.size();
            } else {
                if (dim != streamRecord.getValue().features.size()) {
                    throw new RuntimeException(
                            "The input training data should all have same dimensions.");
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
     * Does machine learning training on the input data with the initialized model, return the
     * trained model and losses.
     *
     * @param trainData The training data.
     * @param initModel The initialized model.
     * @return The trained model and losses during the training.
     */
    private DataStream<LogisticRegressionModelData> train(
            DataStream<LabeledPointWithWeight> trainData, DataStream<double[]> initModel) {
        LogisticGradient logisticGradient = new LogisticGradient(getReg());
        DataStreamList resultList =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(initModel),
                        ReplayableDataStreamList.notReplay(trainData),
                        IterationConfig.newBuilder()
                                .setOperatorLifeCycle(OperatorLifeCycle.ALL_ROUND)
                                .build(),
                        new TrainIterationBody(
                                logisticGradient,
                                getGlobalBatchSize(),
                                getLearningRate(),
                                getMaxIter(),
                                getTol()));
        return resultList.get(0);
    }

    /** The iteration implementation for training process. */
    private static class TrainIterationBody implements IterationBody {

        private final LogisticGradient logisticGradient;

        private final int globalBatchSize;

        private final double learningRate;

        private final int maxIter;

        private final double tol;

        public TrainIterationBody(
                LogisticGradient logisticGradient,
                int globalBatchSize,
                double learningRate,
                int maxIter,
                double tol) {
            this.logisticGradient = logisticGradient;
            this.globalBatchSize = globalBatchSize;
            this.learningRate = learningRate;
            this.maxIter = maxIter;
            this.tol = tol;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            // The variable stream at the first epoch is the initialized model.
            // In the following iterations, it is the computed gradient and loss.
            DataStream<double[]> variableStream = variableStreams.get(0);
            DataStream<LabeledPointWithWeight> trainData = dataStreams.get(0);
            SingleOutputStreamOperator<double[]> gradientAndLoss =
                    trainData
                            .connect(variableStream)
                            .transform(
                                    "updateModelAndComputeGradients",
                                    PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
                                    new CacheDataAndDoTrain(
                                            logisticGradient, globalBatchSize, learningRate));
            // The feedback variable stream is the reduced gradients.
            DataStreamList feedbackVariableStream =
                    IterationBody.forEachRound(
                            DataStreamList.of(gradientAndLoss),
                            input -> {
                                DataStream<double[]> feedback =
                                        DataStreamUtils.allReduceSum(input.get(0));
                                return DataStreamList.of(feedback);
                            });
            DataStream<Integer> terminationCriteria =
                    feedbackVariableStream
                            .get(0)
                            .map(
                                    x -> {
                                        double[] value = (double[]) x;
                                        return value[value.length - 1] / value[value.length - 2];
                                    })
                            .flatMap(new TerminateOnMaxIterOrTol(maxIter, tol));

            return new IterationBodyResult(
                    DataStreamList.of(feedbackVariableStream.get(0)),
                    DataStreamList.of(gradientAndLoss.getSideOutput(MODEL_OUTPUT)),
                    terminationCriteria);
        }
    }

    /**
     * A stream operator that caches the training data in the first iteration and updates model
     * using gradients iteratively. The first input is the training data, and the second input is
     * the initialized model or feedback of gradient and loss.
     */
    private static class CacheDataAndDoTrain extends AbstractStreamOperator<double[]>
            implements TwoInputStreamOperator<LabeledPointWithWeight, double[], double[]>,
                    IterationListener<double[]> {

        private int dim;

        private final int globalBatchSize;

        private int localBatchSize;

        private final double learningRate;

        private final LogisticGradient logisticGradient;

        private Random random = new Random(2021);

        private DenseVector gradient;

        private DenseVector coefficient;

        private ListState<DenseVector> coefficientState;

        /** TODO: add a more efficient sampling method. */
        private List<LabeledPointWithWeight> cachedTrainData;

        private ListState<LabeledPointWithWeight> trainDataState;

        private List<LabeledPointWithWeight> batchData;

        /** The buffer for feedback record: {coefficient, weightSum, loss}. */
        private double[] feedbackBuffer;

        private ListState<double[]> feedbackBufferState;

        private ListState<Double> lossState;

        public CacheDataAndDoTrain(
                LogisticGradient logisticGradient, int globalBatchSize, double learningRate) {
            this.logisticGradient = logisticGradient;
            this.globalBatchSize = globalBatchSize;
            this.learningRate = learningRate;
        }

        @Override
        public void open() {
            int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
            int taskId = getRuntimeContext().getIndexOfThisSubtask();
            localBatchSize = globalBatchSize / numTasks;
            if (globalBatchSize % numTasks > taskId) {
                localBatchSize++;
            }
            this.batchData = new ArrayList<>(localBatchSize);
        }

        private List<LabeledPointWithWeight> getBatchData(
                List<LabeledPointWithWeight> cachedData, int batchSize) {
            batchData.clear();
            for (int i = 0; i < batchSize; i++) {
                batchData.add(cachedData.get(random.nextInt(cachedData.size())));
            }
            return batchData;
        }

        private void updateModel() throws Exception {
            System.arraycopy(feedbackBuffer, 0, gradient.values, 0, gradient.size());
            double weightSum = feedbackBuffer[dim];
            double loss = feedbackBuffer[dim + 1] / weightSum;
            lossState.add(loss);
            BLAS.axpy(-learningRate / weightSum, gradient, coefficient);
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<double[]> collector) {
            // TODO: let this method throws exception.
            if (epochWatermark == 0) {
                // initialize model and allocate memory
                coefficient = new DenseVector(feedbackBuffer);
                dim = coefficient.size();
                feedbackBuffer = new double[dim + 2];
                gradient = new DenseVector(dim);
            } else {
                try {
                    updateModel();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            Arrays.fill(gradient.values, 0);
            try {
                if (cachedTrainData == null) {
                    cachedTrainData = IteratorUtils.toList(trainDataState.get().iterator());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            batchData = getBatchData(cachedTrainData, localBatchSize);
            Tuple2<Double, Double> weightAndLossSum =
                    logisticGradient.computeLoss(batchData, coefficient);
            logisticGradient.computeGradient(batchData, coefficient, gradient);
            System.arraycopy(gradient.values, 0, feedbackBuffer, 0, gradient.size());
            feedbackBuffer[dim] = weightAndLossSum.f0;
            feedbackBuffer[dim + 1] = weightAndLossSum.f1;
            collector.collect(feedbackBuffer);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onIterationTerminated(Context context, Collector collector) {
            // TODO: let this method throws exception.
            // Updates model using the feedback buffer.
            // Note that the gradients are received but onEpochWatermarkIncremented() is not
            // invoked when we met termination condition.
            trainDataState.clear();
            coefficientState.clear();
            feedbackBufferState.clear();
            try {
                if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                    updateModel();
                    context.output(MODEL_OUTPUT, new LogisticRegressionModelData(coefficient));
                    double[] loss =
                            ((List<Double>) IteratorUtils.toList(lossState.get().iterator()))
                                    .stream().mapToDouble(Double::doubleValue).toArray();
                    context.output(LOSS_OUTPUT, loss);
                }
                lossState.clear();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void processElement1(StreamRecord<LabeledPointWithWeight> streamRecord)
                throws Exception {
            trainDataState.add(streamRecord.getValue());
        }

        @Override
        public void processElement2(StreamRecord<double[]> streamRecord) {
            feedbackBuffer = streamRecord.getValue();
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
            lossState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "lossState", BasicTypeInfo.DOUBLE_TYPE_INFO));
            coefficientState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "coefficientState",
                                            TypeInformation.of(DenseVector.class)));
            OperatorStateUtils.getUniqueElement(coefficientState, "coefficientState")
                    .ifPresent(x -> coefficient = x);
            feedbackBufferState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "feedbackBufferState",
                                            PrimitiveArrayTypeInfo
                                                    .DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
            OperatorStateUtils.getUniqueElement(feedbackBufferState, "feedbackBufferState")
                    .ifPresent(x -> feedbackBuffer = x);
            // allocate memory for gradient, initialize dim.
            if (coefficient != null) {
                dim = coefficient.size();
                gradient = new DenseVector(new double[dim]);
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            coefficientState.clear();
            if (coefficient != null) {
                coefficientState.add(coefficient);
            }
            feedbackBufferState.clear();
            if (feedbackBuffer != null) {
                feedbackBufferState.add(feedbackBuffer);
            }
        }
    }
}
