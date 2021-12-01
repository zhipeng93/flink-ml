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

package org.apache.flink.ml.classification.linear;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.feature.LabeledPointWithWeight;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;

import java.io.Serializable;
import java.util.List;

/**
 * Utility class to compute gradient and loss for logistic loss. For details, see
 * http://mlwiki.org/index.php/Logistic_Regression.
 */
public class LogisticGradient implements Serializable {

    /** L2 regularization term. */
    private final double l2;

    public LogisticGradient(double l2) {
        this.l2 = l2;
    }

    /**
     * Computes sum of weight and loss on a set of samples.
     *
     * @param labeledData A sample set of train data.
     * @param coefficient The model parameters.
     * @return Weight and loss sum of the input data.
     */
    public final Tuple2<Double, Double> computeLoss(
            List<LabeledPointWithWeight> labeledData, DenseVector coefficient) {
        double weightSum = 0.0;
        double lossSum = 0.0;
        for (LabeledPointWithWeight dataPoint : labeledData) {
            lossSum += dataPoint.weight * computeLoss(dataPoint, coefficient);
            weightSum += dataPoint.weight;
        }
        if (Double.compare(0, l2) != 0) {
            lossSum += l2 * Math.pow(BLAS.norm2(coefficient), 2);
        }
        return Tuple2.of(weightSum, lossSum);
    }

    /**
     * Computes gradient on a set of samples.
     *
     * @param labeledData A sample set of train data.
     * @param coefficient The model parameters.
     * @param gradient The accumulated gradients.
     * @return Weight sum of the input data.
     */
    public final double computeGradient(
            List<LabeledPointWithWeight> labeledData,
            DenseVector coefficient,
            DenseVector gradient) {
        double weightSum = 0.0;
        for (LabeledPointWithWeight dataPoint : labeledData) {
            weightSum += dataPoint.weight;
            computeGradient(dataPoint, coefficient, gradient);
        }
        if (Double.compare(0, l2) != 0) {
            BLAS.axpy(this.l2 * 2, coefficient, gradient);
        }
        return weightSum;
    }

    private double computeLoss(LabeledPointWithWeight dataPoint, DenseVector coefficient) {
        double dot = BLAS.dot(dataPoint.features, coefficient);
        double labelScaled = 2 * dataPoint.label - 1;
        return Math.log(1 + Math.exp(-dot * labelScaled));
    }

    private void computeGradient(
            LabeledPointWithWeight dataPoint, DenseVector coefficient, DenseVector cumGradient) {
        double dot = BLAS.dot(dataPoint.features, coefficient);
        double labelScaled = 2 * dataPoint.label - 1;
        double multiplier = dataPoint.weight * (-labelScaled / (Math.exp(dot * labelScaled) + 1));
        BLAS.axpy(multiplier, dataPoint.features, cumGradient);
    }
}
