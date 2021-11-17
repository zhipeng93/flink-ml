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

package org.apache.flink.ml.common.datastream;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NumberSequenceIterator;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/** Tests the {@link DataStreamUtils}. */
public class DataStreamUtilsTest {
    private StreamExecutionEnvironment env;

    @Before
    public void before() {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(4);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDistinct() throws Exception {
        DataStream<Double> dataStream =
                env.fromParallelCollection(new NumberSequenceIterator(1L, 10L), Types.LONG)
                        .map(x -> (x / 2) * 1.);
        double[] result =
                ((List<Double>)
                                IteratorUtils.toList(
                                        DataStreamUtils.distinct(dataStream).executeAndCollect()))
                        .stream().mapToDouble(Double::doubleValue).toArray();
        Arrays.sort(result);
        assertArrayEquals(new double[] {0., 1., 2., 3., 4., 5.}, result, 1e-7);
    }

    @Test
    public void testMapPartition() throws Exception {
        DataStream<Long> dataStream =
                env.fromParallelCollection(new NumberSequenceIterator(0L, 19L), Types.LONG);
        DataStream<Integer> countsPerPartition =
                DataStreamUtils.mapPartition(dataStream, new TestMapPartitionFunc());
        List<Integer> counts = IteratorUtils.toList(countsPerPartition.executeAndCollect());
        assertArrayEquals(
                counts.stream().mapToInt(Integer::intValue).toArray(), new int[] {5, 5, 5, 5});
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSortPartition() throws Exception {
        DataStream<Long> dataStream =
                env.fromParallelCollection(new NumberSequenceIterator(0L, 10L), Types.LONG);
        DataStream<Long> output =
                DataStreamUtils.sortPartition(dataStream, new LongComparator(true));
        output.getTransformation().setParallelism(1);
        long[] result =
                ((List<Long>)
                                IteratorUtils.toList(
                                        DataStreamUtils.distinct(dataStream).executeAndCollect()))
                        .stream().mapToLong(Long::longValue).toArray();
        assertArrayEquals(new long[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, result);
    }

    /** A simple implementation for a {@link MapPartitionFunction}. */
    private static class TestMapPartitionFunc implements MapPartitionFunction<Long, Integer> {

        public void mapPartition(Iterable<Long> values, Collector<Integer> out) {
            int cnt = 0;
            for (long ignored : values) {
                cnt++;
            }
            out.collect(cnt);
        }
    }
}
