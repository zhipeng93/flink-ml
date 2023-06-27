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

import org.apache.flink.api.java.tuple.Tuple2;

/** TODO: optimize this impl. */
public class FTRLOptimizer {
    private final double alpha;
    private final double beta;
    private final double lambda1;
    private final double lambda2;

    public FTRLOptimizer(double alpha, double beta, double lambda1, double lambda2) {
        this.alpha = alpha;
        this.beta = beta;
        this.lambda1 = lambda1;
        this.lambda2 = lambda2;
    }

    /**
     * Uses the sparse gradient to update the model.
     *
     * @param gradient
     * @param model
     */
    public void updateModel(Tuple2<long[], double[]> gradient, double[] model, double[] z) {}

    public void updateModel(double[] gradient, double[] model, double[] z) {}
}
