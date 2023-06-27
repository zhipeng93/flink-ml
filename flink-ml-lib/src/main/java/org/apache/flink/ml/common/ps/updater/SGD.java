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

package org.apache.flink.ml.common.ps.updater;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.common.optimizer.RegularizationUtils;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;

import java.util.Collections;
import java.util.Iterator;

/** TODO: optimize this impl. */
public class SGD implements ModelUpdater<Tuple3<Long, Long, double[]>> {
    private final double learningRate;
    private final double reg;
    private final double elasticNet;

    // ------ Model data of FTRL optimizer. -----
    private double[] model;
    private long startKeyIndex;
    private long endKeyIndex;

    public SGD(double learningRate, double reg, double elasticNet) {
        this.learningRate = learningRate;
        this.reg = reg;
        this.elasticNet = elasticNet;
    }

    @Override
    public void open(long startKeyIndex, long endKeyIndex) {
        this.startKeyIndex = startKeyIndex;
        this.endKeyIndex = endKeyIndex;
        this.model = new double[(int) (endKeyIndex - startKeyIndex)];
    }

    @Override
    public void update(long[] keys, double[] values) {
        for (int i = 0; i < keys.length; i++) {
            model[(int) (keys[i] - startKeyIndex)] -= learningRate * values[i];
        }

        RegularizationUtils.regularize(Vectors.dense(model), reg, elasticNet, learningRate);
    }

    @Override
    public double[] get(long[] keys) {
        double[] values = new double[keys.length];
        for (int i = 0; i < keys.length; i++) {
            values[i] = model[(int) (keys[i] - startKeyIndex)];
        }
        return values;
    }

    @Override
    public Iterator<Tuple3<Long, Long, double[]>> getModelSegments() {
        return Collections.singletonList(Tuple3.of(startKeyIndex, endKeyIndex, model)).iterator();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {}

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {}
}
