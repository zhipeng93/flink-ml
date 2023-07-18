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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.statefun.flink.core.queue.Locks;

import java.util.Iterator;

public final class LockFreeBatchFeedbackQueueWithSpill<ElementT>
        implements FeedbackQueueWithSpill<ElementT> {
    private static final int INITIAL_BUFFER_SIZE = 32 * 1024; // 32k

    private final MpscQueueWithSpill<ElementT> queue =
            new MpscQueueWithSpill<>(INITIAL_BUFFER_SIZE, Locks.spinLock());

    @Override
    public boolean addAndCheckIfWasEmpty(ElementT element) {
        final int size = queue.add(element);
        return size == 1;
    }

    @Override
    public Iterator<ElementT> drainAll() {
        return queue.drainAll();
    }

    @Override
    public void setSpillPath(String path, TypeSerializer<ElementT> serializer) {
        queue.setSpillPath(path, serializer);
    }

    @Override
    public void reset() {
        queue.resetStandBy();
    }

    @Override
    public void close() {
        queue.close();
    }
}
