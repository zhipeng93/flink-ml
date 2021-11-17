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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.commons.collections.IteratorUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
     * Collects distinct values in a bounded data stream. The parallelism of the output stream is 1.
     *
     * @param <T> The class type of the input data stream.
     * @param input The bounded input data stream.
     * @return The result data stream that contains all the distinct values.
     */
    public static <T> DataStream<T> distinct(DataStream<T> input) {
        return input.transform(
                        "distinctInEachPartition",
                        input.getType(),
                        new DistinctPartitionOperator<>())
                .setParallelism(input.getParallelism())
                .transform(
                        "distinctInFinalPartition",
                        input.getType(),
                        new DistinctPartitionOperator<>())
                .setParallelism(1);
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
     * Sorts the elements in each partition of the input bounded data stream.
     *
     * @param input The input data stream.
     * @param comparator The comparator used to sort the elements.
     * @param <IN> The class type of input element.
     * @return The sorted data stream.
     */
    public static <IN> DataStream<IN> sortPartition(
            DataStream<IN> input, TypeComparator<IN> comparator) {
        return input.transform(
                        "sortPartition", input.getType(), new SortPartitionOperator<>(comparator))
                .setParallelism(input.getParallelism());
    }

    /**
     * A stream operator to compute the distinct values in each partition of the input bounded data
     * stream.
     */
    private static class DistinctPartitionOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T>, BoundedOneInput {

        private ListState<T> distinctLabelsState;

        private Set<T> distinctLabels = new HashSet<>();

        @Override
        public void endInput() {
            for (T distinctLabel : distinctLabels) {
                output.collect(new StreamRecord<>(distinctLabel));
            }
            distinctLabelsState.clear();
        }

        @Override
        public void processElement(StreamRecord<T> streamRecord) {
            distinctLabels.add(streamRecord.getValue());
        }

        @Override
        @SuppressWarnings("unchecked")
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            distinctLabelsState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "distinctLabels",
                                            getOperatorConfig()
                                                    .getTypeSerializerIn(
                                                            0, getClass().getClassLoader())));
            distinctLabels.clear();
            distinctLabels.addAll(
                    (List<T>) IteratorUtils.toList(distinctLabelsState.get().iterator()));
        }

        @Override
        @SuppressWarnings("unchecked")
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            distinctLabelsState.clear();
            distinctLabelsState.addAll((List<T>) IteratorUtils.toList(distinctLabels.iterator()));
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
                            "input",
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

    /**
     * A stream operator to sort the elements in each partition of the input bounded data stream.
     */
    private static class SortPartitionOperator<IN> extends AbstractStreamOperator<IN>
            implements OneInputStreamOperator<IN, IN>, BoundedOneInput {

        private ListState<IN> valuesState;

        private TypeComparator<IN> comparator;

        public SortPartitionOperator(TypeComparator<IN> comparator) {
            this.comparator = comparator;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            ListStateDescriptor<IN> descriptor =
                    new ListStateDescriptor<>(
                            "input",
                            getOperatorConfig()
                                    .getTypeSerializerIn(0, getClass().getClassLoader()));
            valuesState = context.getOperatorStateStore().getListState(descriptor);
        }

        @Override
        public void endInput() throws Exception {
            List<IN> cachedElements = IteratorUtils.toList(valuesState.get().iterator());
            cachedElements.sort((o1, o2) -> comparator.compare(o1, o2));
            StreamRecord<IN> reused = new StreamRecord<>(null);
            for (IN ele : cachedElements) {
                reused.replace(ele);
                output.collect(reused);
            }
            valuesState.clear();
        }

        @Override
        public void processElement(StreamRecord<IN> input) throws Exception {
            valuesState.add(input.getValue());
        }
    }
}
