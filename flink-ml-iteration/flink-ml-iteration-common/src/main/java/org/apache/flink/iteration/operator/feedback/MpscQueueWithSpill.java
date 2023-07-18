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

package org.apache.flink.iteration.operator.feedback;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.flink.core.queue.Lock;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;

/**
 * Multi producers single consumer fifo queue.
 *
 * <p>This queue supports two operations:
 *
 * <ul>
 *   <li>{@link #add(Object)} atomically adds an element to this queue and returns the number of
 *       elements in the queue after the addition.
 *   <li>{@link #drainAll()} atomically obtains a snapshot of the queue and simultaneously empties
 *       the queue, i.e. drains it.
 * </ul>
 *
 * @param <T> element type
 */
@Internal
public final class MpscQueueWithSpill<T> {

    private static final Deque<?> EMPTY = new ArrayDeque<>(0);

    // -- configuration
    private final Lock lock;

    // -- runtime
    // private ArrayDeque<T> active;
    // private ArrayDeque<T> standby;

    private String spillPath;
    private TypeSerializer serializer;

    private String spillActivePath;
    private String spillStandByPath;

    private SpillableQueue<T> activeQueue;
    private SpillableQueue<T> standByQueue;
    private static final int MAX_IN_MEMORY_NUM = 1000;

    public MpscQueueWithSpill(int initialBufferSize, Lock lock) {
        this.lock = Objects.requireNonNull(lock);
        // this.active = new ArrayDeque<>(initialBufferSize);
        // this.standby = new ArrayDeque<>(initialBufferSize);
    }

    public void setSpillPath(String spillPath, TypeSerializer<T> serializer) {
        Preconditions.checkState(this.spillPath == null && this.serializer == null);
        this.spillPath = spillPath;
        this.serializer = serializer;

        this.spillActivePath = spillPath + "-active";
        this.spillStandByPath = spillPath + "-standby";

        activeQueue =
                new SpillableQueue<>(MAX_IN_MEMORY_NUM, Paths.get(spillActivePath), serializer);
        standByQueue =
                new SpillableQueue<>(MAX_IN_MEMORY_NUM, Paths.get(spillStandByPath), serializer);
    }

    /**
     * Adds an element to this (unbound) queue.
     *
     * @param element the element to add.
     * @return the number of elements in the queue after the addition.
     */
    public int add(T element) {
        Objects.requireNonNull(element);
        final Lock lock = this.lock;
        lock.lockUninterruptibly();

        try {
            SpillableQueue<T> active = this.activeQueue;
            Preconditions.checkState(element instanceof StreamRecord);
            active.add(((StreamRecord<T>) element).getValue());
            return active.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Atomically drains the queue.
     *
     * @return a batch of elements that obtained atomically from that queue.
     */
    public Iterator<T> drainAll() {
        final Lock lock = this.lock;
        lock.lockUninterruptibly();
        try {
            final SpillableQueue<T> ready = this.activeQueue;
            if (ready.isEmpty()) {
                return Collections.emptyIterator();
            }
            // swap active with standby
            this.activeQueue = this.standByQueue;
            this.standByQueue = ready;
            try {
                return ready.getDataIterator();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } finally {
            lock.unlock();
        }
    }

    public void resetStandBy() {
        standByQueue.reset();
    }

    public void close() {
        try {
            activeQueue.close();
            standByQueue.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Deque<T> empty() {
        return (Deque<T>) EMPTY;
    }
}
