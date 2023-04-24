package org.apache.flink.ml.common.broadcast;
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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBody.PerRoundSubBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.IterationConfig.OperatorLifeCycle;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.ml.util.TestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.NumberSequenceIterator;

import org.junit.Test;

import java.util.Collections;

/** Demo case for using withBroadcast in iterations. */
public class IterationWithBroadcastTest {

    @Test
    public void testIterationWithBroadcast() throws Exception {
        StreamExecutionEnvironment env = TestUtils.getExecutionEnvironment();
        DataStream<Long> broadcastStream =
                env.fromParallelCollection(new NumberSequenceIterator(0L, 1L), Types.LONG);
        DataStream<Long> inputStream =
                env.fromParallelCollection(new NumberSequenceIterator(0L, 0L), Types.LONG);
        DataStreamList resultStream =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(inputStream),
                        ReplayableDataStreamList.replay(broadcastStream),
                        IterationConfig.newBuilder()
                                .setOperatorLifeCycle(OperatorLifeCycle.PER_ROUND)
                                .build(),
                        new IterationBodyWithBroadcast());

        resultStream.get(0).addSink(new SinkFunction<Object>() {});
        env.execute();
    }

    private static class IterationBodyWithBroadcast implements IterationBody {

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {

            DataStreamList feedbackVariableStream =
                    IterationBody.forEachRound(
                            dataStreams, new PerRoundIterationBodyWithBroadcast(variableStreams));

            DataStream<Integer> terminationCriteria =
                    feedbackVariableStream
                            .get(0)
                            .flatMap(new TerminateOnMaxIter(3))
                            .returns(Types.INT);

            return new IterationBodyResult(
                    feedbackVariableStream, variableStreams, terminationCriteria);
        }
    }

    private static class PerRoundIterationBodyWithBroadcast implements PerRoundSubBody {

        DataStreamList variableStreams;

        public PerRoundIterationBodyWithBroadcast(DataStreamList variableStreams) {
            this.variableStreams = variableStreams;
        }

        @Override
        public DataStreamList process(DataStreamList inputList) {
            DataStream<Long> broadcastResult =
                    BroadcastUtils.withBroadcastStream(
                                    Collections.singletonList(variableStreams.get(0)),
                                    Collections.singletonMap("broadcast", inputList.get(0)),
                                    inputs -> {
                                        DataStream<Long> data1 = (DataStream<Long>) inputs.get(0);

                                        return data1.map(new MockMap());
                                    })
                            .map(x -> x)
                            .returns(Types.LONG);

            return DataStreamList.of(broadcastResult);
        }
    }

    private static class MockMap extends RichMapFunction<Long, Long> {

        @Override
        public Long map(Long value) throws Exception {
            System.out.println(
                    "broadcast variable : "
                            + getRuntimeContext().getBroadcastVariable("broadcast"));
            System.out.println("received value: " + value);
            return value;
        }
    }
}
