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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Provides utility functions for {@link DataStream}. */
public class DataStreamUtils {
    /**
     * Applies allReduceSum on the input data stream. The input data stream is supposed to contain
     * one double array in each partition. The result data stream has the same parallelism as the
     * input, where each partition contains one double array that sums all of the double arrays in
     * the input data stream.
     *
     * <p>Note that we throw exception when one of the following two cases happen:
     * <li>There exists one partition that contains more than one double array.
     * <li>The length of the double array is not consistent among all partitions.
     *
     * @param input The input data stream.
     * @return The result data stream.
     */
    public static DataStream<double[]> allReduceSum(DataStream<double[]> input) {
        return AllReduceImpl.allReduceSum(input);
    }

    /**
     * Applies a {@link MapPartitionFunction} on a bounded data stream.
     *
     * @param input The input data stream.
     * @param func The user defined mapPartition function.
     * @param <IN> The class type of the input element.
     * @param <OUT> The class type of output element.
     * @return The result data stream.
     */
    public static <IN, OUT> DataStream<OUT> mapPartition(
            DataStream<IN> input, MapPartitionFunction<IN, OUT> func) {
        TypeInformation<OUT> resultType =
                TypeExtractor.getMapPartitionReturnTypes(func, input.getType(), null, true);
        return input.transform("mapPartition", resultType, new MapPartitionOperator<>(func))
                .setParallelism(input.getParallelism());
    }

    /**
     * Applies a {@link ReduceFunction} on a bounded data stream.
     *
     * @param input The input data stream.
     * @param func The user defined reduce function.
     * @param <IN> The class type of the input element.
     * @return The result data stream.
     */
    public static <IN> DataStream<IN> reduce(DataStream<IN> input, ReduceFunction<IN> func) {
        return input.transform("reduceInPartition", input.getType(), new ReduceOperator(func))
                .transform("reduceAll", input.getType(), new ReduceOperator(func))
                .setParallelism(1);
    }

    /**
     * A stream operator to apply {@link ReduceFunction} on each partition of the input bounded data
     * stream.
     */
    private static class ReduceOperator<IN> extends AbstractStreamOperator<IN>
            implements OneInputStreamOperator<IN, IN>, BoundedOneInput {

        private final ReduceFunction<IN> func;

        private ListState<IN> reducedState;
        private IN reduceVal;

        public ReduceOperator(ReduceFunction<IN> func) {
            this.func = func;
        }

        @Override
        public void endInput() throws Exception {
            output.collect(new StreamRecord<>(reduceVal));
            reducedState.clear();
        }

        @Override
        public void processElement(StreamRecord<IN> streamRecord) throws Exception {
            if (reduceVal == null) {
                reduceVal = streamRecord.getValue();
            } else {
                reduceVal = func.reduce(reduceVal, streamRecord.getValue());
            }
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            ListStateDescriptor<IN> descriptor =
                    new ListStateDescriptor<>(
                            "reducedState",
                            getOperatorConfig()
                                    .getTypeSerializerIn(0, getClass().getClassLoader()));
            reducedState = context.getOperatorStateStore().getListState(descriptor);
            reduceVal =
                    OperatorStateUtils.getUniqueElement(reducedState, "reducedState").orElse(null);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            reducedState.clear();
            reducedState.add(reduceVal);
        }
    }

    /**
     * A stream operator to apply {@link MapPartitionFunction} on each partition of the input
     * bounded data stream.
     */
    private static class MapPartitionOperator<IN, OUT> extends AbstractStreamOperator<OUT>
            implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

        private final MapPartitionFunction<IN, OUT> mapPartitionFunc;

        private ListState<IN> valuesState;

        public MapPartitionOperator(MapPartitionFunction<IN, OUT> mapPartitionFunc) {
            this.mapPartitionFunc = mapPartitionFunc;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            ListStateDescriptor<IN> descriptor =
                    new ListStateDescriptor<>(
                            "inputState",
                            getOperatorConfig()
                                    .getTypeSerializerIn(0, getClass().getClassLoader()));
            valuesState = context.getOperatorStateStore().getListState(descriptor);
        }

        @Override
        public void endInput() throws Exception {
            mapPartitionFunc.mapPartition(valuesState.get(), new TimestampedCollector<>(output));
            valuesState.clear();
        }

        @Override
        public void processElement(StreamRecord<IN> input) throws Exception {
            valuesState.add(input.getValue());
        }
    }
}
