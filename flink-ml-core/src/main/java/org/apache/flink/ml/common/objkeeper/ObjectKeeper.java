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

package org.apache.flink.ml.common.objkeeper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.AbstractID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A utility class to maintain static variables such that multiple co-located operators can share.
 *
 * <p>Note that the 'object' is shared by all tasks on the same task manager, users should guarantee
 * that no two tasks modify a 'object' at the same time.
 */
public class ObjectKeeper {

    private static final ConcurrentHashMap<Tuple2<String, Integer>, Object> STATES =
            new ConcurrentHashMap<>();

    private static AtomicLong handle = new AtomicLong(0);

    private static final Logger LOG = LoggerFactory.getLogger(ObjectKeeper.class);

    public static void put(Tuple2<String, Integer> key, Object state) {
        LOG.info("taskId: {}, Put handle: {}", key.f1, key.f0);
        STATES.put(key, state);
    }

    @SuppressWarnings("unchecked")
    public static <T> T get(Tuple2<String, Integer> key) {
        LOG.info("taskId: {}, Get handle: {}", key.f1, key.f0);
        return (T) STATES.get(key);
    }

    @SuppressWarnings("unchecked")
    public static <T> T remove(Tuple2<String, Integer> key) {
        LOG.info("taskId: {}, Remove handle: {}", key.f1, key.f0);
        return (T) STATES.remove(key);
    }

    public static String getNewKey(AbstractID abstractID) {
        return abstractID.toHexString() + "-" + (handle.getAndIncrement());
    }
}
