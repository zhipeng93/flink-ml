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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.Model;
import org.apache.flink.ml.common.broadcast.BroadcastUtils;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** This class implements {@link Model} for {@link LogisticRegression}. */
public class LogisticRegressionModel
        implements Model<LogisticRegressionModel>,
                LogisticRegressionModelParams<LogisticRegressionModel> {

    private Map<Param<?>, Object> paramMap = new HashMap<>();

    private Table model;

    public LogisticRegressionModel() {
        ParamUtils.initializeMapWithDefaultValues(this.paramMap, this);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
        ReadWriteUtils.saveModelData(
                LogisticRegressionModelData.getModelDataStream(model),
                path,
                LogisticRegressionModelData.getModelDataEncoder());
    }

    public static LogisticRegressionModel load(StreamExecutionEnvironment env, String path)
            throws IOException {
        LogisticRegressionModel model = ReadWriteUtils.loadStageParam(path);
        Table modelData =
                ReadWriteUtils.loadModelData(
                        env, path, LogisticRegressionModelData.getModelDataDecoder());
        return model.setModelData(modelData);
    }

    @Override
    public LogisticRegressionModel setModelData(Table... inputs) {
        model = inputs[0];
        return this;
    }

    @Override
    public Table[] getModelData() {
        return new Table[] {model};
    }

    @Override
    @SuppressWarnings("unchecked")
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Row> data = tEnv.toDataStream(inputs[0]);
        final String broadcastModelKey = "broadcastModel";
        DataStream<LogisticRegressionModelData> modelData =
                LogisticRegressionModelData.getModelDataStream(model);
        DataStream<Row> predictResult =
                BroadcastUtils.withBroadcastStream(
                        Collections.singletonList(data),
                        Collections.singletonMap(broadcastModelKey, modelData),
                        inputList -> {
                            DataStream inputData = inputList.get(0);
                            return inputData.transform(
                                    "doPrediction",
                                    new RowTypeInfo(
                                            new TypeInformation[] {
                                                TypeInformation.of(DenseVector.class),
                                                BasicTypeInfo.DOUBLE_TYPE_INFO,
                                                PrimitiveArrayTypeInfo
                                                        .DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO
                                            },
                                            new String[] {
                                                getFeaturesCol(),
                                                getPredictionCol(),
                                                getRawPredictionCol()
                                            }),
                                    new PredictOp(
                                            new PredictOneRecord(
                                                    getFeaturesCol(), broadcastModelKey)));
                        });

        return new Table[] {tEnv.fromDataStream(predictResult)};
    }

    /** A utility operator used for prediction. */
    private static class PredictOp extends AbstractUdfStreamOperator<Row, RichMapFunction<Row, Row>>
            implements OneInputStreamOperator<Row, Row> {
        public PredictOp(RichMapFunction<Row, Row> userFunction) {
            super(userFunction);
        }

        @Override
        public void processElement(StreamRecord<Row> streamRecord) throws Exception {
            output.collect(new StreamRecord<>(userFunction.map(streamRecord.getValue())));
        }
    }

    /** A utility function used to predict one input record. */
    private static class PredictOneRecord extends RichMapFunction<Row, Row> {

        String broadcastModelKey;

        String featuresCol;

        DenseVector coefficient;

        public PredictOneRecord(String featuresCol, String broadcastModelKey) {
            this.featuresCol = featuresCol;
            this.broadcastModelKey = broadcastModelKey;
        }

        @Override
        public Row map(Row value) {
            if (coefficient == null) {
                LogisticRegressionModelData modelData =
                        (LogisticRegressionModelData)
                                getRuntimeContext().getBroadcastVariable(broadcastModelKey).get(0);
                coefficient = modelData.coefficient;
            }
            DenseVector features = (DenseVector) value.getField(featuresCol);
            Tuple2<Double, double[]> pred = predictWithProb(features, coefficient);
            return Row.of(features, pred.f0, pred.f1);
        }
    }

    /**
     * The main logic that predicts one input record.
     *
     * @param feature The input feature.
     * @param coefficient The model parameters.
     * @return The prediction label and the detail probabilities.
     */
    private static Tuple2<Double, double[]> predictWithProb(
            DenseVector feature, DenseVector coefficient) {
        double dotValue = BLAS.dot(feature, coefficient);
        double prob = 1 - 1.0 / (1.0 + Math.exp(dotValue));
        return new Tuple2<>(dotValue >= 0 ? 1. : 0., new double[] {1 - prob, prob});
    }
}
