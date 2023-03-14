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

package org.apache.flink.ml.classification.logisticregression;

import org.apache.flink.ml.api.AlgoOperator;

/**
 * An AlgoOperator which implements the large scale logistic regression algorithm.
 *
 * <p>See https://en.wikipedia.org/wiki/Logistic_regression.
 */
public abstract class PSFtrl implements AlgoOperator<PSFtrl>, PSFtrlParams<PSFtrl> {

    // private final Map<Param<?>, Object> paramMap = new HashMap<>();
    //
    // public PSFtrl() {
    //    ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    // }
    //
    // @Override
    // public Table[] transform(Table... inputs) {
    //    Preconditions.checkArgument(inputs.length == 1);
    //    String classificationType = getMultiClass();
    //    Preconditions.checkArgument(
    //            "auto".equals(classificationType) || "binomial".equals(classificationType),
    //            "Multinomial classification is not supported yet. Supported options: [auto,
    // binomial].");
    //    StreamTableEnvironment tEnv =
    //            (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
    //
    //    DataStream<LabeledLargePointWithWeight> trainData =
    //            tEnv.toDataStream(inputs[0])
    //                    .map(
    //                            dataPoint -> {
    //                                double weight =
    //                                        getWeightCol() == null
    //                                                ? 1.0
    //                                                : ((Number)
    // dataPoint.getField(getWeightCol()))
    //                                                        .doubleValue();
    //                                double label =
    //                                        ((Number) dataPoint.getField(getLabelCol()))
    //                                                .doubleValue();
    //                                boolean isBinomial =
    //                                        Double.compare(0., label) == 0
    //                                                || Double.compare(1., label) == 0;
    //                                if (!isBinomial) {
    //                                    throw new RuntimeException(
    //                                            "Multinomial classification is not supported yet.
    // Supported options: [auto, binomial].");
    //                                }
    //                                SparseLongDoubleVector features =
    //                                        ((SparseLongDoubleVector)
    //                                                dataPoint.getField(getFeaturesCol()));
    //                                return new LabeledLargePointWithWeight(features, label,
    // weight);
    //                            });
    //
    //    // The input
    //    DataStream<Long> initModelData =
    //            DataStreamUtils.reduce(
    //                            trainData.map(
    //                                    x -> x.features.indices[x.features.indices.length - 1]),
    //                            (ReduceFunction<Long>) Math::max)
    //                    .map(x -> x + 1);
    //
    //    PSSGD pssgd =
    //            new PSSGD(
    //                    getNumPs(),
    //                    getMaxIter(),
    //                    getLearningRate(),
    //                    getGlobalBatchSize(),
    //                    getTol(),
    //                    getReg(),
    //                    getElasticNet());
    //    DataStream<Tuple4<Integer, Long, Long, double[]>> rawModelData =
    //            pssgd.optimize(initModelData, trainData, BinaryLogisticLoss.INSTANCE);
    //
    //    Table outputModel =
    //            tEnv.fromDataStream(rawModelData)
    //                    .as("model_id", "start_index", "end_index", "model");
    //    return new Table[] {outputModel};
    // }
    //
    // @Override
    // public void save(String path) throws IOException {
    //    ReadWriteUtils.saveMetadata(this, path);
    // }
    //
    // public static PSFtrl load(StreamTableEnvironment tEnv, String path) throws IOException {
    //    return ReadWriteUtils.loadStageParam(path);
    // }
    //
    // @Override
    // public Map<Param<?>, Object> getParamMap() {
    //    return paramMap;
    // }
}
