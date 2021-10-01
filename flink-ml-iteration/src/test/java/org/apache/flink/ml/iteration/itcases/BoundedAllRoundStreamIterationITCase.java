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

package org.apache.flink.ml.iteration.itcases;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.iteration.DataStreamList;
import org.apache.flink.ml.iteration.IterationBody;
import org.apache.flink.ml.iteration.IterationBodyResult;
import org.apache.flink.ml.iteration.Iterations;
import org.apache.flink.ml.iteration.compile.DraftExecutionEnvironment;
import org.apache.flink.ml.iteration.itcases.operators.OutputRecord;
import org.apache.flink.ml.iteration.itcases.operators.RoundBasedTerminationCriteria;
import org.apache.flink.ml.iteration.itcases.operators.SequenceSource;
import org.apache.flink.ml.iteration.itcases.operators.TwoInputReduceAllRoundProcessFunction;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.ml.iteration.itcases.UnboundedStreamIterationITCase.createMiniClusterConfiguration;
import static org.apache.flink.ml.iteration.itcases.UnboundedStreamIterationITCase.createVariableAndConstantJobGraph;
import static org.apache.flink.ml.iteration.itcases.UnboundedStreamIterationITCase.createVariableOnlyJobGraph;
import static org.apache.flink.ml.iteration.itcases.UnboundedStreamIterationITCase.verifyResult;
import static org.junit.Assert.assertEquals;

/**
 * Tests the cases of {@link Iterations#iterateBoundedStreamsUntilTermination(DataStreamList,
 * DataStreamList, IterationBody)}.
 */
public class BoundedAllRoundStreamIterationITCase {

    private static BlockingQueue<OutputRecord<Integer>> result = new LinkedBlockingQueue<>();

    @Before
    public void setup() {
        result.clear();
    }

    @Test(timeout = 60000)
    public void testSyncVariableOnlyBoundedIteration() throws Exception {
        try (MiniCluster miniCluster = new MiniCluster(createMiniClusterConfiguration(2, 2))) {
            miniCluster.start();

            // Create the test job
            JobGraph jobGraph =
                    createVariableOnlyJobGraph(
                            4,
                            1000,
                            false,
                            0,
                            true,
                            4,
                            new SinkFunction<OutputRecord<Integer>>() {
                                @Override
                                public void invoke(OutputRecord<Integer> value, Context context) {
                                    result.add(value);
                                }
                            });
            miniCluster.executeJobBlocking(jobGraph);

            assertEquals(6, result.size());

            Map<Integer, Tuple2<Integer, Integer>> roundsStat = new HashMap<>();
            for (int i = 0; i < 5; ++i) {
                OutputRecord<Integer> next = result.take();
                assertEquals(OutputRecord.Event.EPOCH_WATERMARK_INCREMENTED, next.getEvent());
                Tuple2<Integer, Integer> state =
                        roundsStat.computeIfAbsent(next.getRound(), ignored -> new Tuple2<>(0, 0));
                state.f0++;
                state.f1 = next.getValue();
            }

            verifyResult(roundsStat, 5, 1, 4 * (0 + 999) * 1000 / 2);
            assertEquals(OutputRecord.Event.TERMINATED, result.take().getEvent());
        }
    }

    @Test(timeout = 60000)
    public void testSyncVariableAndConstantBoundedIteration() throws Exception {
        try (MiniCluster miniCluster = new MiniCluster(createMiniClusterConfiguration(2, 2))) {
            miniCluster.start();

            // Create the test job
            JobGraph jobGraph =
                    createVariableAndConstantJobGraph(
                            4,
                            1000,
                            false,
                            0,
                            true,
                            4,
                            new SinkFunction<OutputRecord<Integer>>() {
                                @Override
                                public void invoke(OutputRecord<Integer> value, Context context) {
                                    result.add(value);
                                }
                            });
            miniCluster.executeJobBlocking(jobGraph);

            assertEquals(6, result.size());

            Map<Integer, Tuple2<Integer, Integer>> roundsStat = new HashMap<>();
            for (int i = 0; i < 5; ++i) {
                OutputRecord<Integer> next = result.take();
                assertEquals(OutputRecord.Event.EPOCH_WATERMARK_INCREMENTED, next.getEvent());
                Tuple2<Integer, Integer> state =
                        roundsStat.computeIfAbsent(next.getRound(), ignored -> new Tuple2<>(0, 0));
                state.f0++;
                state.f1 = next.getValue();
            }

            verifyResult(roundsStat, 5, 1, 4 * (0 + 999) * 1000 / 2);
            assertEquals(OutputRecord.Event.TERMINATED, result.take().getEvent());
        }
    }

    @Test
    public void testTerminationCriteria() throws Exception {
        try (MiniCluster miniCluster = new MiniCluster(createMiniClusterConfiguration(2, 2))) {
            miniCluster.start();

            // Create the test job
            JobGraph jobGraph =
                    createJobGraphWithTerminationCriteria(
                            4,
                            1000,
                            false,
                            0,
                            true,
                            4,
                            new SinkFunction<OutputRecord<Integer>>() {
                                @Override
                                public void invoke(OutputRecord<Integer> value, Context context) {
                                    result.add(value);
                                }
                            });
            miniCluster.executeJobBlocking(jobGraph);

            assertEquals(6, result.size());

            Map<Integer, Tuple2<Integer, Integer>> roundsStat = new HashMap<>();
            for (int i = 0; i < 5; ++i) {
                OutputRecord<Integer> next = result.take();
                assertEquals(OutputRecord.Event.EPOCH_WATERMARK_INCREMENTED, next.getEvent());
                Tuple2<Integer, Integer> state =
                        roundsStat.computeIfAbsent(next.getRound(), ignored -> new Tuple2<>(0, 0));
                state.f0++;
                state.f1 = next.getValue();
            }

            verifyResult(roundsStat, 5, 1, 4 * (0 + 999) * 1000 / 2);
            assertEquals(OutputRecord.Event.TERMINATED, result.take().getEvent());
        }
    }

    static JobGraph createJobGraphWithTerminationCriteria(
            int numSources,
            int numRecordsPerSource,
            boolean holdSource,
            int period,
            boolean sync,
            int maxRound,
            SinkFunction<OutputRecord<Integer>> sinkFunction) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Integer> variableSource =
                env.addSource(new DraftExecutionEnvironment.EmptySource<Integer>() {})
                        .setParallelism(numSources)
                        .name("Variable");
        DataStream<Integer> constSource =
                env.addSource(new SequenceSource(numRecordsPerSource, holdSource, period))
                        .setParallelism(numSources)
                        .name("Constants");
        DataStreamList outputs =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(variableSource),
                        DataStreamList.of(constSource),
                        (variableStreams, dataStreams) -> {
                            SingleOutputStreamOperator<Integer> reducer =
                                    variableStreams
                                            .<Integer>get(0)
                                            .connect(dataStreams.<Integer>get(0))
                                            .process(
                                                    new TwoInputReduceAllRoundProcessFunction(
                                                            sync, maxRound * 10));
                            return new IterationBodyResult(
                                    DataStreamList.of(
                                            reducer.map(x -> x).setParallelism(numSources)),
                                    DataStreamList.of(
                                            reducer.getSideOutput(
                                                    new OutputTag<OutputRecord<Integer>>(
                                                            "output") {})),
                                    reducer.flatMap(new RoundBasedTerminationCriteria(maxRound)));
                        });
        outputs.<OutputRecord<Integer>>get(0).addSink(sinkFunction);

        return env.getStreamGraph().getJobGraph();
    }
}
