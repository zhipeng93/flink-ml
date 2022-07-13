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

package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;

/** Demo for onlineKmeans. */
public class OnlineKmeansDemo {
    static int modelVersion = 0;

    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "localhost");
        int port = tool.getInt("port", 9999);
        int p = tool.getInt("parallelism", 1);
        int globalBatchSize = tool.getInt("globalBatchSize", 1);
        int inputDataDim = tool.getInt("inputDataDim", 2);
        int k = tool.getInt("k", 2);
        double decayFactor = tool.getDouble("decayFactor", 0.5);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(p);

        DataStreamSource<String> source = env.socketTextStream(host, port);
        DataStream<DenseVector> points =
                source.flatMap(
                        new FlatMapFunction<String, DenseVector>() {
                            @Override
                            public void flatMap(String value, Collector<DenseVector> out)
                                    throws Exception {
                                String[] splitted = value.split("\\s+");
                                if (splitted.length != inputDataDim) {
                                    return;
                                }
                                double[] doubleValues = new double[inputDataDim];
                                for (int i = 0; i < inputDataDim; i++) {
                                    doubleValues[i] = Double.parseDouble(splitted[i]);
                                }
                                out.collect(Vectors.dense(doubleValues));
                            }
                        });
        Table input = tEnv.fromDataStream(points).as("features");
        OnlineKMeans onlineKMeans =
                new OnlineKMeans()
                        .setFeaturesCol("features")
                        .setK(k)
                        .setGlobalBatchSize(globalBatchSize)
                        .setDecayFactor(decayFactor)
                        .setInitialModelData(
                                KMeansModelData.generateRandomModelData(
                                        tEnv, k, inputDataDim, 1, 2022));
        Table modelDataTable = onlineKMeans.fit(input).getModelData()[0];
        DataStream<KMeansModelData> modelData = KMeansModelData.getModelDataStream(modelDataTable);

        modelData.addSink(
                new SinkFunction<KMeansModelData>() {

                    @Override
                    public void invoke(KMeansModelData value, Context context) throws Exception {
                        System.out.println(kmeansModelDataToString(value));
                    }
                });
        env.execute("sink");
    }

    private static String kmeansModelDataToString(KMeansModelData modelData) {
        StringBuilder sb = new StringBuilder();
        sb.append("Model version-" + (modelVersion++) + ":\n");
        DecimalFormat decimalFormat = new DecimalFormat("0.000");
        for (int i = 0; i < modelData.weights.size(); i++) {
            double weight = modelData.weights.get(i);
            double[] center = modelData.centroids[i].values;
            sb.append(i + "-th center: \t([");
            int idxMax = center.length - 1;
            for (int idx = 0; idx <= idxMax; idx++) {
                sb.append(decimalFormat.format(center[idx]));
                if (idx == idxMax) {
                    sb.append(']');
                } else {
                    sb.append(", ");
                }
            }
            sb.append(", " + weight + ")\n");
        }

        return sb.toString();
    }
}
