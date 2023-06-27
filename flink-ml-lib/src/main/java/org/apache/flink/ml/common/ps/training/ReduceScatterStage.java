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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nullable;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** A communication stage that conducts reduce-scatter on the given array. */
public final class ReduceScatterStage<V> implements IterationStage {
    public final Supplier<V[]> sendBuf;
    public final Consumer<V[]> recvBuf;
    /**
     * The number of elements each worker receives. If not specified, the scattered result are
     * evenly distributed to all workers.
     */
    @Nullable public int[] recvCounts;

    public final ReduceFunction<V[]> reducer;
    public final TypeSerializer<V> typeSerializer;

    /**
     * Defines how often this stage is executed in terms of number of iterations. If execution
     * interval is 5, then this stage is only executed when iteration id is 5, 10, 15, etc.
     */
    public final int executionInterval;

    public ReduceScatterStage(
            Supplier<V[]> sendBuf,
            Consumer<V[]> recvBuf,
            int[] recvCounts,
            ReduceFunction<V[]> reducer,
            TypeSerializer<V> typeSerializer,
            int executionInterval) {
        this.sendBuf = sendBuf;
        this.recvBuf = recvBuf;
        this.recvCounts = recvCounts;
        this.reducer = reducer;
        this.typeSerializer = typeSerializer;
        this.executionInterval = executionInterval;
    }

    public ReduceScatterStage(
            Supplier<V[]> sendBuf,
            Consumer<V[]> recvBuf,
            int[] recvCounts,
            ReduceFunction<V[]> reducer,
            TypeSerializer<V> typeSerializer) {
        this(sendBuf, recvBuf, recvCounts, reducer, typeSerializer, 1);
    }
}
