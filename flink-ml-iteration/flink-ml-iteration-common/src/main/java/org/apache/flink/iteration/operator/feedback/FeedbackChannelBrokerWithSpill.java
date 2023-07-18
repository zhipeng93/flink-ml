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

import org.apache.flink.statefun.flink.core.feedback.FeedbackChannel;
import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HandOffChannelBroker.
 *
 * <p>It is used together with the co-location constrain so that two tasks can access the same
 * "hand-off" channel, and communicate directly (not via the network stack) by simply passing
 * references in one direction.
 *
 * <p>To obtain a feedback channel one must first obtain an {@link SubtaskFeedbackKey} and simply
 * call {@link #get()}. A channel is removed from this broker on a call to {@link
 * FeedbackChannel#close()}.
 */
public final class FeedbackChannelBrokerWithSpill {

    private static final FeedbackChannelBrokerWithSpill INSTANCE =
            new FeedbackChannelBrokerWithSpill();

    private final ConcurrentHashMap<SubtaskFeedbackKey<?>, FeedbackChannelWithSpill<?>> channels =
            new ConcurrentHashMap<>();

    public static FeedbackChannelBrokerWithSpill get() {
        return INSTANCE;
    }

    @SuppressWarnings({"unchecked"})
    public <V> FeedbackChannelWithSpill<V> getChannel(SubtaskFeedbackKey<V> key) {
        Objects.requireNonNull(key);

        FeedbackChannelWithSpill<?> channel =
                channels.computeIfAbsent(key, FeedbackChannelBrokerWithSpill::newChannel);

        return (FeedbackChannelWithSpill<V>) channel;
    }

    @SuppressWarnings("resource")
    void removeChannel(SubtaskFeedbackKey<?> key) {
        channels.remove(key);
    }

    private static <V> FeedbackChannelWithSpill<V> newChannel(SubtaskFeedbackKey<V> key) {
        FeedbackQueueWithSpill<V> queue = new LockFreeBatchFeedbackQueueWithSpill<>();
        return new FeedbackChannelWithSpill<>(key, queue);
    }
}
