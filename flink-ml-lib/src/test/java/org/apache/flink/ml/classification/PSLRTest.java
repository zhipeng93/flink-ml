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

package org.apache.flink.ml.classification;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.iteration.IterationRecord;
import org.apache.flink.iteration.operator.InputOperator;
import org.apache.flink.iteration.typeinfo.IterationRecordTypeInfo;
import org.apache.flink.ml.classification.logisticregression.PSLR;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.linalg.SparseLongDoubleVector;
import org.apache.flink.ml.linalg.typeinfo.SparseLongDoubleVectorTypeInfo;
import org.apache.flink.ml.util.Bits;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.NumberSequenceIterator;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests {@link PSLR}. */
public class PSLRTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    private StreamExecutionEnvironment env;

    private StreamTableEnvironment tEnv;
    private final double[] expectedCoefficient =
            new double[] {
                0.28373777275207973, -0.6602391558940176, -0.5023144275785298, -0.37632776953777075
            };

    private static final List<Row> binomialSparseTrainData =
            Arrays.asList(
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 1}, new double[] {1, 2}),
                            0.,
                            1.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 2}, new double[] {2, 3}),
                            0.,
                            2.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 3}, new double[] {3, 4}),
                            0.,
                            3.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 2}, new double[] {4, 4}),
                            0.,
                            4.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 1}, new double[] {5, 4}),
                            0.,
                            5.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 2}, new double[] {11, 3}),
                            1.,
                            1.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 3}, new double[] {12, 4}),
                            1.,
                            2.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 1}, new double[] {13, 2}),
                            1.,
                            3.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 3}, new double[] {14, 4}),
                            1.,
                            4.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 2}, new double[] {15, 4}),
                            1.,
                            5.));

    private Table binomialSparseDataTable;

    @Before
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setParallelism(4);
        // env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);
        binomialSparseDataTable =
                tEnv.fromDataStream(
                        env.fromCollection(
                                binomialSparseTrainData,
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            SparseLongDoubleVectorTypeInfo.INSTANCE,
                                            Types.DOUBLE,
                                            Types.DOUBLE
                                        },
                                        new String[] {"features", "label", "weight"})));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPSLR() throws Exception {
        env.setParallelism(5);
        int numPss = 5;
        PSLR pslr =
                new PSLR().setWeightCol("weight").setMaxIter(20).setNumPs(numPss).setSyncMode(true);
        Table model = pslr.transform(binomialSparseDataTable)[0];
        List<Row> modelData = IteratorUtils.toList(tEnv.toDataStream(model).executeAndCollect());

        assertEquals(numPss, modelData.size());

        modelData.sort(Comparator.comparingLong(o -> o.getFieldAs(1)));
        double[] collectedCoefficient = new double[4];
        for (Row piece : modelData) {
            int startIndex = ((Long) piece.getFieldAs(1)).intValue();
            double[] pieceCoeff = piece.getFieldAs(3);
            System.arraycopy(pieceCoeff, 0, collectedCoefficient, startIndex, pieceCoeff.length);
        }
        System.out.println(Arrays.toString(collectedCoefficient));
        assertArrayEquals(expectedCoefficient, collectedCoefficient, 1e-7);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPSLRWithModelDim() throws Exception {
        env.setParallelism(5);
        int numPss = 5;
        PSLR pslr =
                new PSLR()
                        .setModelDim(4)
                        .setWeightCol("weight")
                        .setMaxIter(20)
                        .setNumPs(numPss)
                        .setSyncMode(true);
        Table model = pslr.transform(binomialSparseDataTable)[0];
        List<Row> modelData = IteratorUtils.toList(tEnv.toDataStream(model).executeAndCollect());

        assertEquals(numPss, modelData.size());

        modelData.sort(Comparator.comparingLong(o -> o.getFieldAs(1)));
        double[] collectedCoefficient = new double[4];
        for (Row piece : modelData) {
            int startIndex = ((Long) piece.getFieldAs(1)).intValue();
            double[] pieceCoeff = piece.getFieldAs(3);
            System.arraycopy(pieceCoeff, 0, collectedCoefficient, startIndex, pieceCoeff.length);
        }
        System.out.println(Arrays.toString(collectedCoefficient));
        assertArrayEquals(expectedCoefficient, collectedCoefficient, 1e-7);
    }

    @Test
    public void testPSLRAsync() throws Exception {
        env.setParallelism(4);
        int numPss = 2;
        PSLR pslr =
                new PSLR()
                        .setWeightCol("weight")
                        .setMaxIter(20)
                        .setNumPs(numPss)
                        .setSyncMode(false);
        Table model = pslr.transform(binomialSparseDataTable)[0];
        List<Row> modelData = IteratorUtils.toList(tEnv.toDataStream(model).executeAndCollect());

        assertEquals(numPss, modelData.size());

        modelData.sort(Comparator.comparingLong(o -> o.getFieldAs(1)));
        double[] collectedCoefficient = new double[4];
        for (Row piece : modelData) {
            int startIndex = ((Long) piece.getFieldAs(1)).intValue();
            double[] pieceCoeff = piece.getFieldAs(3);
            System.arraycopy(pieceCoeff, 0, collectedCoefficient, startIndex, pieceCoeff.length);
        }
        System.out.println(Arrays.toString(collectedCoefficient));
        // assertArrayEquals(expectedCoefficient, collectedCoefficient, 1e-7);
    }

    @Test
    public void miniClusterTest() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT, "18081-19091");
        configuration.set(
                ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(1)
                        .build();

        MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration);
        miniCluster.start();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.getConfig().enableObjectReuse();

        int p = 1;
        env.setParallelism(p);
        env.setBufferTimeout(10);
        int numWorkers = p;
        int numPss = p;
        final String fileName = "/Users/zhangzp/root/env/odps/flink_ml_lr_medium_10000.txt";
        final FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(fileName))
                        .build();
        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        DataStream<Row> inputData =
                stream.map(
                                new MapFunction<String, Row>() {
                                    @Override
                                    public Row map(String line) throws Exception {
                                        String[] contents = line.split(",");
                                        int length = contents.length;
                                        long groupId = Long.parseLong(contents[0]);
                                        long featureLen = Long.parseLong(contents[1]);
                                        long label = Long.parseLong(contents[length - 2]);
                                        long transformedLabel = label == 1 ? 1 : 0;
                                        long[] nnzIndices = new long[length - 4];
                                        for (int i = 2; i < nnzIndices.length + 2; i++) {
                                            nnzIndices[i - 2] = Long.parseLong(contents[i]);
                                        }
                                        double[] values = new double[nnzIndices.length];
                                        Arrays.fill(values, 1.0);
                                        return Row.of(
                                                transformedLabel,
                                                new SparseLongDoubleVector(
                                                        featureLen, nnzIndices, values));
                                    }
                                })
                        .returns(
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            Types.LONG, SparseLongDoubleVectorTypeInfo.INSTANCE
                                        },
                                        new String[] {"label", "features"}));

        PSLR pslr =
                new PSLR()
                        .setGlobalBatchSize(numWorkers * 500)
                        .setMaxIter(10)
                        .setNumPs(numPss)
                        .setAlpha(0.1)
                        .setBeta(1.0)
                        .setReg(2.0)
                        .setElasticNet(0.5)
                        .setSyncMode(true);

        Table modelData = pslr.transform(tEnv.fromDataStream(inputData))[0];
        tEnv.toDataStream(modelData)
                .addSink(
                        new SinkFunction<Row>() {
                            @Override
                            public void invoke(Row value) throws Exception {
                                SinkFunction.super.invoke(value);
                                StringBuilder sb = new StringBuilder();
                                sb.append("model_id: ");
                                sb.append(value.getField(0));
                                sb.append(" , start_index: ");
                                sb.append(value.getField(1));
                                sb.append(", end_index: ");
                                sb.append(value.getField(2));
                                System.out.println(sb);
                            }
                        });

        miniCluster.executeJobBlocking(env.getStreamGraph().getJobGraph());
    }

    @Test
    public void e2eTest() throws Exception {
        env.setParallelism(12);
        env.setBufferTimeout(10);
        int numWorkers = 12;
        int numPss = 6;
        final String fileName = "/Users/zhangzp/root/env/odps/flink_ml_lr_medium_10000.txt";
        final FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(fileName))
                        .build();
        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        DataStream<Row> inputData =
                stream.map(
                                new MapFunction<String, Row>() {
                                    @Override
                                    public Row map(String line) throws Exception {
                                        String[] contents = line.split(",");
                                        int length = contents.length;
                                        long groupId = Long.parseLong(contents[0]);
                                        long featureLen = Long.parseLong(contents[1]);
                                        long label = Long.parseLong(contents[length - 2]);
                                        long transformedLabel = label == 1 ? 1 : 0;
                                        long[] nnzIndices = new long[length - 4];
                                        for (int i = 2; i < nnzIndices.length + 2; i++) {
                                            nnzIndices[i - 2] = Long.parseLong(contents[i]);
                                        }
                                        double[] values = new double[nnzIndices.length];
                                        Arrays.fill(values, 1.0);
                                        return Row.of(
                                                transformedLabel,
                                                new SparseLongDoubleVector(
                                                        featureLen, nnzIndices, values));
                                    }
                                })
                        .returns(
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            Types.LONG, SparseLongDoubleVectorTypeInfo.INSTANCE
                                        },
                                        new String[] {"label", "features"}));

        PSLR pslr =
                new PSLR()
                        .setGlobalBatchSize(numWorkers * 500)
                        .setMaxIter(100)
                        .setNumPs(numPss)
                        .setAlpha(0.1)
                        .setBeta(1.0)
                        .setReg(2.0)
                        .setElasticNet(0.5)
                        .setSyncMode(false);

        Table modelData = pslr.transform(tEnv.fromDataStream(inputData))[0];
        tEnv.toDataStream(modelData)
                .addSink(
                        new SinkFunction<Row>() {
                            @Override
                            public void invoke(Row value) throws Exception {
                                SinkFunction.super.invoke(value);
                                String sb =
                                        "model_id: "
                                                + value.getField(0)
                                                + " , start_index: "
                                                + value.getField(1)
                                                + ", end_index: "
                                                + value.getField(2);
                                System.out.println(sb);
                            }
                        });
        env.execute();
    }

    @Test
    public void test100() throws Exception {
        for (int i = 0; i < 100; i++) {
            testPSLR();
            System.out.println(i);
        }
    }

    @Test
    public void testBroadcast() throws Exception {
        env.setParallelism(900);
        env.getConfig().enableObjectReuse();
        DataStream<Long> input =
                env.fromParallelCollection(new NumberSequenceIterator(1L, 100L), Types.LONG);
        DataStream<Long> sum =
                DataStreamUtils.reduce(input, (ReduceFunction<Long>) Long::sum, Types.LONG);

        DataStream<byte[]> output =
                sum.broadcast()
                        .map(
                                new MapFunction<Long, byte[]>() {
                                    @Override
                                    public byte[] map(Long value) throws Exception {
                                        byte[] bytes = new byte[Long.BYTES];
                                        Bits.putLong(bytes, 0, value);
                                        return bytes;
                                    }
                                });

        DataStream<IterationRecord<byte[]>> result =
                output.transform(
                        "Input",
                        new IterationRecordTypeInfo(
                                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
                        new InputOperator<byte[]>());
        List<byte[]> collectedResult = IteratorUtils.toList(result.executeAndCollect());
        System.out.println(collectedResult.size());
    }
}
