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

package org.apache.flink.ml.common.broadcast;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.iteration.compile.DraftExecutionEnvironment;
import org.apache.flink.ml.common.broadcast.operator.BroadcastVariableReceiverOperatorFactory;
import org.apache.flink.ml.common.broadcast.operator.BroadcastWrapper;
import org.apache.flink.ml.common.broadcast.operator.DefaultWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/** Utility class to support withBroadcast in DataStream. */
public class BroadcastUtils {
    /**
     * Supports withBroadcastStream in DataStream API. Broadcast data streams are available at all
     * parallel instances of an operator that extends {@code
     * org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator<OUT, ? extends
     * org.apache.flink.api.common.functions.RichFunction>}. Users can access the broadcast
     * variables by {@code RichFunction.getRuntimeContext().getBroadcastVariable(...)} or {@code
     * RichFunction.getRuntimeContext().hasBroadcastVariable(...)} or {@code
     * RichFunction.getRuntimeContext().getBroadcastVariableWithInitializer(...)}.
     *
     * <p>In detail, the broadcast input data streams will be consumed first and further consumed by
     * non-broadcast inputs. For now the non-broadcast input are cached by default to avoid the
     * possible deadlocks.
     *
     * @param inputList Non-broadcast input list.
     * @param bcStreams Map of the broadcast data streams, where the key is the name and the value
     *     is the corresponding data stream.
     * @param userDefinedFunction The user defined logic in which users can access the broadcast
     *     data streams and produce the output data stream. Note that though users can add more than
     *     one operator in this logic, but only the operator that generates the result stream can
     *     contain a rich function and access the broadcast variables.
     * @return The output data stream.
     */
    public static <OUT> DataStream<OUT> withBroadcastStream(
            List<DataStream<?>> inputList,
            Map<String, DataStream<?>> bcStreams,
            Function<List<DataStream<?>>, DataStream<OUT>> userDefinedFunction) {
        Preconditions.checkArgument(inputList.size() > 0);

        StreamExecutionEnvironment env = inputList.get(0).getExecutionEnvironment();
        String[] broadcastNames = new String[bcStreams.size()];
        DataStream<?>[] broadcastInputs = new DataStream[bcStreams.size()];
        TypeInformation<?>[] broadcastInTypes = new TypeInformation[bcStreams.size()];
        int idx = 0;
        final String broadcastId = new AbstractID().toHexString();
        for (String name : bcStreams.keySet()) {
            broadcastNames[idx] = broadcastId + "-" + name;
            broadcastInputs[idx] = bcStreams.get(name);
            broadcastInTypes[idx] = broadcastInputs[idx].getType();
            idx++;
        }

        DataStream<OUT> resultStream =
                getResultStream(env, inputList, broadcastNames, userDefinedFunction);
        TypeInformation<OUT> outType = resultStream.getType();
        final String coLocationKey = "broadcast-co-location-" + UUID.randomUUID();
        DataStream<OUT> cachedBroadcastInputs =
                cacheBroadcastVariables(
                        env,
                        broadcastNames,
                        broadcastInputs,
                        broadcastInTypes,
                        resultStream.getParallelism(),
                        outType);

        boolean canCoLocate =
                cachedBroadcastInputs.getTransformation() instanceof PhysicalTransformation
                        && resultStream.getTransformation() instanceof PhysicalTransformation;
        if (canCoLocate) {
            ((PhysicalTransformation<?>) cachedBroadcastInputs.getTransformation())
                    .setChainingStrategy(ChainingStrategy.HEAD);
            ((PhysicalTransformation<?>) resultStream.getTransformation())
                    .setChainingStrategy(ChainingStrategy.HEAD);
        } else {
            throw new UnsupportedOperationException(
                    "cannot set chaining strategy on "
                            + cachedBroadcastInputs.getTransformation()
                            + " and "
                            + resultStream.getTransformation()
                            + ".");
        }
        cachedBroadcastInputs.getTransformation().setCoLocationGroupKey(coLocationKey);
        resultStream.getTransformation().setCoLocationGroupKey(coLocationKey);

        return cachedBroadcastInputs.union(resultStream);
    }

    /**
     * Caches all broadcast input data streams in static variables and returns the result
     * multi-input stream operator. The result multi-input stream operator emits nothing and the
     * only functionality of this operator is to cache all the input records in ${@link
     * BroadcastContext}.
     *
     * @param env Execution environment.
     * @param broadcastInputNames Names of the broadcast input data streams.
     * @param broadcastInputs List of the broadcast data streams.
     * @param broadcastInTypes Output types of the broadcast input data streams.
     * @param parallelism Parallelism.
     * @param outType Output type.
     * @param <OUT> Output type.
     * @return The result multi-input stream operator.
     */
    private static <OUT> DataStream<OUT> cacheBroadcastVariables(
            StreamExecutionEnvironment env,
            String[] broadcastInputNames,
            DataStream<?>[] broadcastInputs,
            TypeInformation<?>[] broadcastInTypes,
            int parallelism,
            TypeInformation<OUT> outType) {
        MultipleInputTransformation<OUT> transformation =
                new MultipleInputTransformation<>(
                        "broadcastInputs",
                        new BroadcastVariableReceiverOperatorFactory<>(
                                broadcastInputNames, broadcastInTypes),
                        outType,
                        parallelism);
        for (DataStream<?> dataStream : broadcastInputs) {
            transformation.addInput(dataStream.broadcast().getTransformation());
        }
        env.addOperator(transformation);
        return new MultipleConnectedStreams(env).transform(transformation);
    }

    /**
     * Uses {@link DraftExecutionEnvironment} to execute the userDefinedFunction and returns the
     * resultStream.
     *
     * @param env Execution environment.
     * @param inputList Non-broadcast input list.
     * @param broadcastStreamNames Names of the broadcast data streams.
     * @param graphBuilder User-defined logic.
     * @param <OUT> Output type of the result stream.
     * @return The result stream by applying user-defined logic on the input list.
     */
    private static <OUT> DataStream<OUT> getResultStream(
            StreamExecutionEnvironment env,
            List<DataStream<?>> inputList,
            String[] broadcastStreamNames,
            Function<List<DataStream<?>>, DataStream<OUT>> graphBuilder) {

        // Executes the graph builder and gets real non-broadcast inputs.
        DraftExecutionEnvironment draftEnv =
                new DraftExecutionEnvironment(env, new DefaultWrapper<>());

        List<DataStream<?>> draftSources = new ArrayList<>();
        for (DataStream<?> dataStream : inputList) {
            draftSources.add(draftEnv.addDraftSource(dataStream, dataStream.getType()));
        }
        DataStream<OUT> draftOutStream = graphBuilder.apply(draftSources);

        List<Transformation<?>> realNonBroadcastInputs =
                draftOutStream.getTransformation().getInputs();
        TypeInformation<?>[] inTypes = new TypeInformation[realNonBroadcastInputs.size()];
        for (int i = 0; i < realNonBroadcastInputs.size(); i++) {
            inTypes[i] = realNonBroadcastInputs.get(i).getOutputType();
        }
        // Does not block all non-broadcast input edges by default.
        boolean[] isBlocked = new boolean[realNonBroadcastInputs.size()];
        Arrays.fill(isBlocked, false);

        // Executes the graph builder and adds the operators to real execution environment.
        DraftExecutionEnvironment draftEnv2 =
                new DraftExecutionEnvironment(
                        env, new BroadcastWrapper<>(broadcastStreamNames, inTypes, isBlocked));
        List<DataStream<?>> draftSources2 = new ArrayList<>();
        for (DataStream<?> dataStream : inputList) {
            draftSources2.add(draftEnv2.addDraftSource(dataStream, dataStream.getType()));
        }
        DataStream<OUT> draftOutStream2 = graphBuilder.apply(draftSources2);
        draftEnv2.copyToActualEnvironment();
        return draftEnv2.getActualStream(draftOutStream2.getId());
    }
}
