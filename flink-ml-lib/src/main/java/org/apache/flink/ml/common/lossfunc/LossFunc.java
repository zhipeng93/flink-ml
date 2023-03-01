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

package org.apache.flink.ml.common.lossfunc;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.common.feature.LabeledLargePointWithWeight;
import org.apache.flink.ml.common.feature.LabeledPointWithWeight;
import org.apache.flink.ml.linalg.SparseLongDoubleVector;
import org.apache.flink.ml.linalg.Vector;

import java.io.Serializable;

/**
 * A loss function is to compute the loss and gradient with the given coefficient and training data.
 */
@Internal
public interface LossFunc extends Serializable {

    /**
     * Computes the loss on the given data point.
     *
     * @param dataPoint A training data point.
     * @param coefficient The model parameters.
     * @return The loss of the input data.
     */
    double computeLoss(LabeledPointWithWeight dataPoint, Vector coefficient);

    /**
     * Computes the gradient on the given data point and adds the computed gradient to cumGradient.
     *
     * @param dataPoint A training data point.
     * @param coefficient The model parameters.
     * @param cumGradient The accumulated gradient.
     */
    void computeGradient(LabeledPointWithWeight dataPoint, Vector coefficient, Vector cumGradient);

    /**
     * Computes the loss on the given data point.
     *
     * @param dataPoint A training data point.
     * @param coefficient The model parameters.
     * @return The loss of the input data.
     */
    default double computeLoss(
            LabeledLargePointWithWeight dataPoint, SparseLongDoubleVector coefficient) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Computes the gradient on the given data point and adds the computed gradient to cumGradient.
     *
     * @param dataPoint A training data point.
     * @param coefficient The model parameters.
     * @param cumGradient The accumulated gradient.
     */
    default void computeGradient(
            LabeledLargePointWithWeight dataPoint,
            SparseLongDoubleVector coefficient,
            SparseLongDoubleVector cumGradient) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
