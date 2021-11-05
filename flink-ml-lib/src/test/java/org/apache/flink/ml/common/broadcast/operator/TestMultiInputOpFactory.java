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

package org.apache.flink.ml.common.broadcast.operator;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.util.List;

/** Factory class for {@link TestMultiInputOp}. */
public class TestMultiInputOpFactory extends AbstractStreamOperatorFactory<Integer> {

    private int numInputs;

    private String[] broadcastNames;

    private final List<List<Integer>> expectedBroadcastInputs;

    public TestMultiInputOpFactory(
            int numInputs, String[] broadcastNames, List<List<Integer>> expectedBroadcastInputs) {
        this.numInputs = numInputs;
        this.broadcastNames = broadcastNames;
        this.expectedBroadcastInputs = expectedBroadcastInputs;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Integer>> T createStreamOperator(
            StreamOperatorParameters<Integer> streamOperatorParameters) {
        return (T)
                new TestMultiInputOp(
                        streamOperatorParameters,
                        numInputs,
                        broadcastNames,
                        expectedBroadcastInputs);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return TestMultiInputOp.class;
    }
}
