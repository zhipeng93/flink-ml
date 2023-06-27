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

package org.apache.flink.ml.common.ps.training;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.ml.common.ps.updater.ModelUpdater;
import org.apache.flink.ml.linalg.DenseIntDoubleVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.DenseIntDoubleVectorTypeInfo;
import org.apache.flink.ml.util.TestUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests {@link TrainingUtils}. */
public class TrainingUtilsTest {
    private static final int NUM_WORKERS = 2;
    private static final int NUM_SERVERS = 2;
    private static final int MAX_ITER = 3;
    private static final int NUM_COLUMNS_PER_KEY = 2;
    private DataStream<Long> maxKey;
    private DataStream<DenseIntDoubleVector> inputData;
    StreamExecutionEnvironment env;

    @Before
    public void before() {
        env = TestUtils.getExecutionEnvironment();
        env.setParallelism(NUM_WORKERS);
        maxKey = env.fromElements(3L);
        inputData =
                env.fromCollection(
                                Arrays.asList(
                                        Vectors.dense(1, 1, 1, 1),
                                        Vectors.dense(2, 2, 2, 2),
                                        Vectors.dense(3, 3, 3, 3),
                                        Vectors.dense(4, 4, 4, 4)))
                        .map(x -> x, DenseIntDoubleVectorTypeInfo.INSTANCE);
    }

    @Test
    public void push() throws Exception {
        MockSession mockSession =
                new MockSession(DenseIntDoubleVectorTypeInfo.INSTANCE, Collections.emptyList());

        IterationStageList<MockSession> stageList =
                new IterationStageList<>(mockSession)
                        .addStage(
                                new PushStage(
                                        (SerializableSupplier<long[]>) () -> new long[] {1, 3},
                                        (SerializableSupplier<double[]>)
                                                () -> new double[] {1, 1, 1, 1}))
                        .setTerminationCriteria(session -> session.iterationId >= MAX_ITER);

        DataStreamList resultList =
                TrainingUtils.train(
                        inputData,
                        stageList,
                        maxKey,
                        new TupleTypeInfo<>(
                                Types.LONG,
                                Types.LONG,
                                PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO),
                        new MockModelUpdater(NUM_COLUMNS_PER_KEY),
                        NUM_SERVERS);
        DataStream<Tuple3<Long, Long, double[]>> modelStream = resultList.get(0);

        List<Tuple3<Long, Long, double[]>> collectedModelData =
                IteratorUtils.toList(modelStream.executeAndCollect());
        double[] modelData = mergeSegment(collectedModelData, NUM_SERVERS);
        double[] expectedModel = new double[] {0, 0, 6, 6, 0, 0, 6, 6};
        assertArrayEquals(expectedModel, modelData, 1e-7);
    }

    @Test
    public void pull() throws Exception {
        MockSession mockSession =
                new MockSession(
                        DenseIntDoubleVectorTypeInfo.INSTANCE,
                        Collections.singletonList(
                                new OutputTag<>(
                                        "pulledResult",
                                        new TupleTypeInfo<>(
                                                Types.INT,
                                                Types.INT,
                                                PrimitiveArrayTypeInfo
                                                        .DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO))));

        IterationStageList<MockSession> stageList =
                new IterationStageList<>(mockSession)
                        .addStage(
                                new PushStage(
                                        (SerializableSupplier<long[]>) () -> new long[] {1, 3},
                                        (SerializableSupplier<double[]>)
                                                () -> new double[] {1, 1, 1, 1}))
                        .addStage(
                                new PullStage(
                                        (SerializableSupplier<long[]>) () -> new long[] {1, 3},
                                        (SerializableConsumer<double[]>)
                                                x -> mockSession.pulledValues = x))
                        .addStage(
                                new MockOutputStage<>(
                                        (SerializableSupplier<double[]>)
                                                () -> mockSession.pulledValues,
                                        1))
                        .setTerminationCriteria(session -> session.iterationId >= MAX_ITER);

        DataStreamList resultList =
                TrainingUtils.train(
                        inputData,
                        stageList,
                        maxKey,
                        new TupleTypeInfo<>(
                                Types.LONG,
                                Types.LONG,
                                PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO),
                        new MockModelUpdater(NUM_COLUMNS_PER_KEY),
                        NUM_SERVERS);

        DataStream<Tuple3<Integer, Integer, double[]>> pulledStream = resultList.get(1);
        List<Tuple3<Integer, Integer, double[]>> pulls =
                IteratorUtils.toList(pulledStream.executeAndCollect());

        List<Tuple3<Integer, Integer, double[]>> expectedPulls = new ArrayList<>();
        for (int i = 0; i < MAX_ITER; i++) {
            double[] value = new double[4];
            for (int w = 0; w < NUM_WORKERS; w++) {
                Arrays.fill(value, (i + 1) * 2);
                expectedPulls.add(Tuple3.of(i, w, value));
            }
        }
        verify(expectedPulls, pulls);
    }

    @Test
    public void allReduce() throws Exception {
        ExecutionConfig executionConfig = maxKey.getExecutionEnvironment().getConfig();
        int executionInterval = 2;
        TypeSerializer<MockPojo> mockPojoTypeSerializer =
                Types.POJO(MockPojo.class).createSerializer(executionConfig);
        MockSession mockSession =
                new MockSession(
                        DenseIntDoubleVectorTypeInfo.INSTANCE,
                        Collections.singletonList(
                                new OutputTag<>(
                                        "allreduce",
                                        new TupleTypeInfo<>(
                                                Types.INT,
                                                Types.INT,
                                                Types.OBJECT_ARRAY(Types.POJO(MockPojo.class))))));

        IterationStageList<MockSession> stageList =
                new IterationStageList<>(mockSession)
                        .addStage(new MockInitStage())
                        .addStage(
                                new AllReduceStage<>(
                                        (SerializableSupplier<MockPojo[]>)
                                                () -> mockSession.allReduceBuffer,
                                        (SerializableConsumer<MockPojo[]>)
                                                x -> mockSession.allReduceBuffer = x,
                                        (ReduceFunction<MockPojo[]>) TrainingUtilsTest::sumPojo,
                                        mockPojoTypeSerializer,
                                        executionInterval))
                        .addStage(
                                new MockOutputStage<>(
                                        (SerializableSupplier<MockPojo[]>)
                                                () -> mockSession.allReduceBuffer,
                                        executionInterval))
                        .setTerminationCriteria(session -> session.iterationId >= MAX_ITER);

        DataStreamList resultList =
                TrainingUtils.train(
                        inputData,
                        stageList,
                        maxKey,
                        new TupleTypeInfo<>(
                                Types.LONG,
                                Types.LONG,
                                PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO),
                        new MockModelUpdater(NUM_COLUMNS_PER_KEY),
                        NUM_SERVERS);
        DataStream<Tuple3<Integer, Integer, MockPojo[]>> allreduceStream = resultList.get(1);

        List<Tuple3<Integer, Integer, MockPojo[]>> allreduce =
                IteratorUtils.toList(allreduceStream.executeAndCollect());

        List<Tuple3<Integer, Integer, MockPojo[]>> expectedAllReduce =
                Arrays.asList(
                        Tuple3.of(0, 0, new MockPojo[] {new MockPojo(2, 4), new MockPojo(2, 4)}),
                        Tuple3.of(0, 1, new MockPojo[] {new MockPojo(2, 4), new MockPojo(2, 4)}),
                        Tuple3.of(2, 0, new MockPojo[] {new MockPojo(4, 8), new MockPojo(4, 8)}),
                        Tuple3.of(2, 1, new MockPojo[] {new MockPojo(4, 8), new MockPojo(4, 8)}));

        verify(expectedAllReduce, allreduce);
    }

    @Test
    public void reduceScatter() throws Exception {
        ExecutionConfig executionConfig = maxKey.getExecutionEnvironment().getConfig();
        int executionInterval = 2;
        TypeSerializer<MockPojo> mockPojoTypeSerializer =
                Types.POJO(MockPojo.class).createSerializer(executionConfig);
        MockSession mockSession =
                new MockSession(
                        DenseIntDoubleVectorTypeInfo.INSTANCE,
                        Collections.singletonList(
                                new OutputTag<>(
                                        "reduceScatter",
                                        new TupleTypeInfo<>(
                                                Types.INT,
                                                Types.INT,
                                                Types.OBJECT_ARRAY(Types.POJO(MockPojo.class))))));

        IterationStageList<MockSession> stageList =
                new IterationStageList<>(mockSession)
                        .addStage(new MockInitStage())
                        .addStage(
                                new ReduceScatterStage<>(
                                        (SerializableSupplier<MockPojo[]>)
                                                () -> mockSession.reduceScatterBuffer,
                                        (SerializableConsumer<MockPojo[]>)
                                                x -> mockSession.reduceScatterBuffer = x,
                                        new int[] {1, 1},
                                        (ReduceFunction<MockPojo[]>) TrainingUtilsTest::sumPojo,
                                        mockPojoTypeSerializer,
                                        executionInterval))
                        .addStage(
                                new MockOutputStage<>(
                                        (SerializableSupplier<MockPojo[]>)
                                                () -> mockSession.reduceScatterBuffer,
                                        executionInterval))
                        .setTerminationCriteria(session -> session.iterationId >= MAX_ITER);

        DataStreamList resultList =
                TrainingUtils.train(
                        inputData,
                        stageList,
                        maxKey,
                        new TupleTypeInfo<>(
                                Types.LONG,
                                Types.LONG,
                                PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO),
                        new MockModelUpdater(NUM_COLUMNS_PER_KEY),
                        NUM_SERVERS);
        DataStream<Tuple3<Integer, Integer, MockPojo[]>> allreduceStream = resultList.get(1);

        List<Tuple3<Integer, Integer, MockPojo[]>> allreduce =
                IteratorUtils.toList(allreduceStream.executeAndCollect());

        List<Tuple3<Integer, Integer, MockPojo[]>> expectedAllReduce =
                Arrays.asList(
                        Tuple3.of(0, 0, new MockPojo[] {new MockPojo(2, 4)}),
                        Tuple3.of(0, 1, new MockPojo[] {new MockPojo(2, 4)}),
                        Tuple3.of(2, 0, new MockPojo[] {new MockPojo(2, 4)}),
                        Tuple3.of(2, 1, new MockPojo[] {new MockPojo(2, 4)}));

        verify(expectedAllReduce, allreduce);
    }

    private static MockPojo[] sumPojo(MockPojo[] d1, MockPojo[] d2) {
        Preconditions.checkArgument(d1.length == d2.length);
        for (int i = 0; i < d1.length; i++) {
            d2[i].i += d1[i].i;
            d2[i].j += d1[i].j;
        }
        return d2;
    }

    private static class MockSession extends MiniBatchMLSession<DenseIntDoubleVector> {

        public MockPojo[] allReduceBuffer;

        public MockPojo[] reduceScatterBuffer;
        private ProxySideOutput output;

        @Override
        public void setOutput(ProxySideOutput collector) {
            this.output = collector;
        }

        public MockSession(
                TypeInformation<DenseIntDoubleVector> typeInformation,
                List<OutputTag<?>> outputTags) {
            super(0, typeInformation, outputTags);
        }
    }

    /** The logic on servers. */
    private static class MockModelUpdater implements ModelUpdater<Tuple3<Long, Long, double[]>> {
        private final int numDoublesPerKey;
        private long startIndex;
        private long endIndex;
        private double[] model;

        private ListState<Long> boundaryState;
        private ListState<double[]> modelDataState;

        public MockModelUpdater(int numDoublesPerKey) {
            this.numDoublesPerKey = numDoublesPerKey;
        }

        @Override
        public void open(long startKeyIndex, long endKeyIndex) {
            this.startIndex = startKeyIndex;
            this.endIndex = endKeyIndex;
            this.model = new double[(int) (endKeyIndex - startKeyIndex) * numDoublesPerKey];
        }

        @Override
        public void update(long[] keys, double[] values) {
            Preconditions.checkState(keys.length * numDoublesPerKey == values.length);
            for (int i = 0; i < keys.length; i++) {
                int index = (int) (keys[i] - startIndex);
                for (int j = 0; j < numDoublesPerKey; j++) {
                    model[index * numDoublesPerKey + j] += values[i * numDoublesPerKey + j];
                }
            }
        }

        @Override
        public double[] get(long[] keys) {
            double[] values = new double[keys.length * numDoublesPerKey];
            for (int i = 0; i < keys.length; i++) {
                int index = (int) (keys[i] - startIndex);
                for (int j = 0; j < numDoublesPerKey; j++) {
                    values[i * numDoublesPerKey + j] += model[index * numDoublesPerKey + j];
                }
            }
            return values;
        }

        @Override
        public Iterator<Tuple3<Long, Long, double[]>> getModelSegments() {
            return Collections.singleton(Tuple3.of(startIndex, endIndex, model)).iterator();
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            boundaryState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("BoundaryState", Types.LONG));

            Iterator<Long> iterator = boundaryState.get().iterator();
            if (iterator.hasNext()) {
                startIndex = iterator.next();
                endIndex = iterator.next();
            }

            modelDataState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "modelDataState",
                                            PrimitiveArrayTypeInfo
                                                    .DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
            Iterator<double[]> modelData = modelDataState.get().iterator();
            if (modelData.hasNext()) {
                model = modelData.next();
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            if (model != null) {
                boundaryState.clear();
                boundaryState.add(startIndex);
                boundaryState.add(endIndex);

                modelDataState.clear();
                modelDataState.add(model);
            }
        }
    }

    private static class MockInitStage extends ProcessStage<MockSession> {

        @Override
        public void process(MockSession session) {
            if (session.iterationId == 0) {
                session.allReduceBuffer = new MockPojo[2];
                session.allReduceBuffer[0] = new MockPojo(1, 2);
                session.allReduceBuffer[1] = new MockPojo(1, 2);
            }

            session.reduceScatterBuffer = new MockPojo[2];
            session.reduceScatterBuffer[0] = new MockPojo(1, 2);
            session.reduceScatterBuffer[1] = new MockPojo(1, 2);
        }
    }

    private static class MockOutputStage<T> extends ProcessStage<MockSession> {

        private final Supplier<T> outputSupplier;
        private final int executionInterval;

        public MockOutputStage(Supplier<T> outputSupplier, int executionInterval) {
            this.outputSupplier = outputSupplier;
            this.executionInterval = executionInterval;
        }

        @Override
        public void process(MockSession session) {
            if (session.iterationId % executionInterval == 0) {
                OutputTag<Tuple3<Integer, Integer, T>> outputTag =
                        (OutputTag<Tuple3<Integer, Integer, T>>) session.getOutputTags().get(0);
                session.output.output(
                        outputTag,
                        new StreamRecord<>(
                                Tuple3.of(
                                        session.iterationId,
                                        session.workerId,
                                        outputSupplier.get())));
            }
        }
    }

    private double[] mergeSegment(List<Tuple3<Long, Long, double[]>> segments, int numSegments) {
        assertEquals(numSegments, segments.size());
        segments.sort(Comparator.comparingLong(x -> x.f0));
        int maxKey = (segments.get(segments.size() - 1).f1).intValue();
        double[] merged = new double[maxKey * NUM_COLUMNS_PER_KEY];
        for (Tuple3<Long, Long, double[]> segment : segments) {
            try {
                System.arraycopy(
                        segment.f2,
                        0,
                        merged,
                        segment.f0.intValue() * NUM_COLUMNS_PER_KEY,
                        segment.f2.length);
            } catch (Exception e) {
                System.out.println();
            }
        }
        return merged;
    }

    private static <V> void verify(
            List<Tuple3<Integer, Integer, V>> expected, List<Tuple3<Integer, Integer, V>> actual) {
        Comparator<Tuple3<Integer, Integer, V>> comparator =
                (o1, o2) -> {
                    int cmp = Integer.compare(o1.f0, o2.f0);
                    if (cmp == 0) {
                        return Integer.compare(o1.f1, o2.f1);
                    } else {
                        return cmp;
                    }
                };

        expected.sort(comparator);
        actual.sort(comparator);

        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            Tuple3<Integer, Integer, V> p = actual.get(i);
            Tuple3<Integer, Integer, V> ep = expected.get(i);
            assertEquals(p.f0, ep.f0);
            assertEquals(p.f1, ep.f1);
            if (p.f2 instanceof double[]) {
                assertArrayEquals((double[]) p.f2, (double[]) ep.f2, 1e-7);
            } else {
                MockPojo[] mp = (MockPojo[]) p.f2;
                MockPojo[] emp = (MockPojo[]) ep.f2;
                assertEquals(emp.length, mp.length);
                for (int m = 0; m < mp.length; m++) {
                    assertEquals(emp[m], mp[m]);
                }
            }
        }
    }
}
