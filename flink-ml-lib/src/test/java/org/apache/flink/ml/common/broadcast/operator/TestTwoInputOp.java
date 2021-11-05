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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility class used for testing {@link TwoInputBroadcastWrapperOperator}. */
public class TestTwoInputOp extends AbstractStreamOperator<Integer>
        implements TwoInputStreamOperator<Integer, Integer, Integer>, HasBroadcastVariable {

    private final String[] broadcastNames;

    private final List<List<Integer>> expectedBroadcastInputs;

    Map<String, List<?>> broadcastVariables = new HashMap<>();

    public TestTwoInputOp(String[] broadcastNames, List<List<Integer>> expectedSizes) {
        this.broadcastNames = broadcastNames;
        this.expectedBroadcastInputs = expectedSizes;
    }

    @Override
    public void setBroadcastVariable(String name, List<?> broadcastVariable) {
        broadcastVariables.put(name, broadcastVariable);
    }

    @Override
    public void processElement1(StreamRecord<Integer> streamRecord) {
        for (int i = 0; i < broadcastNames.length; i++) {
            BroadcastVariableReceiverOperatorTest.compareLists(
                    expectedBroadcastInputs.get(i), broadcastVariables.get(broadcastNames[i]));
        }
        output.collect(streamRecord);
    }

    @Override
    public void processElement2(StreamRecord<Integer> streamRecord) {
        for (int i = 0; i < broadcastNames.length; i++) {
            BroadcastVariableReceiverOperatorTest.compareLists(
                    expectedBroadcastInputs.get(i), broadcastVariables.get(broadcastNames[i]));
        }
        output.collect(streamRecord);
    }
}
