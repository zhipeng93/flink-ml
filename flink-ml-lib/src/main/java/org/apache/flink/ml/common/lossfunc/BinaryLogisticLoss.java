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
import org.apache.flink.ml.classification.logisticregression.LogisticRegression;
import org.apache.flink.ml.common.feature.LabeledLargePointWithWeight;
import org.apache.flink.ml.common.feature.LabeledPointWithWeight;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.SparseLongDoubleVector;
import org.apache.flink.ml.linalg.Vector;

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

/** The loss function for binary logistic loss. See {@link LogisticRegression} for example. */
@Internal
public class BinaryLogisticLoss implements LossFunc {
    public static final BinaryLogisticLoss INSTANCE = new BinaryLogisticLoss();

    private BinaryLogisticLoss() {}

    @Override
    public double computeLoss(LabeledPointWithWeight dataPoint, Vector coefficient) {
        double dot = BLAS.dot(dataPoint.getFeatures(), coefficient);
        double labelScaled = 2 * dataPoint.getLabel() - 1;
        return dataPoint.getWeight() * Math.log(1 + Math.exp(-dot * labelScaled));
    }

    @Override
    public void computeGradient(
            LabeledPointWithWeight dataPoint, Vector coefficient, Vector cumGradient) {
        double dot = BLAS.dot(dataPoint.getFeatures(), coefficient);
        double labelScaled = 2 * dataPoint.getLabel() - 1;
        double multiplier =
                dataPoint.getWeight() * (-labelScaled / (Math.exp(dot * labelScaled) + 1));
        BLAS.axpy(multiplier, dataPoint.getFeatures(), cumGradient, dataPoint.getFeatures().size());
    }

    @Override
    public double computeLossWithDot(
            LabeledLargePointWithWeight dataPoint, Long2DoubleOpenHashMap coefficient, double dot) {
        // double dot = dot(dataPoint.features, coefficient);
        double labelScaled = 2 * dataPoint.label - 1;
        return dataPoint.weight * Math.log(1 + Math.exp(-dot * labelScaled));
    }

    @Override
    public void computeGradientWithDot(
            LabeledLargePointWithWeight dataPoint,
            Long2DoubleOpenHashMap coefficient,
            Long2DoubleOpenHashMap cumGradient,
            double dot) {
        // double dot = dot(dataPoint.features, coefficient);
        double labelScaled = 2 * dataPoint.label - 1;
        double multiplier = dataPoint.weight * (-labelScaled / (Math.exp(dot * labelScaled) + 1));
        axpy(multiplier, dataPoint.features, cumGradient);
    }

    public static double dot(SparseLongDoubleVector features, Long2DoubleOpenHashMap coefficient) {
        double dot = 0;
        for (int i = 0; i < features.indices.length; i++) {
            dot += features.values[i] * coefficient.get(features.indices[i]);
        }
        return dot;
    }

    private static void axpy(double a, SparseLongDoubleVector x, Long2DoubleOpenHashMap y) {
        double z;
        for (int i = 0; i < x.indices.length; i++) {
            z = x.values[i] * a + y.getOrDefault(x.indices[i], 0.);
            y.put(x.indices[i], z);
        }
    }
}
