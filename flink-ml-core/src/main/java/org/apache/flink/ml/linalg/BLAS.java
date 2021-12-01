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

package org.apache.flink.ml.linalg;

import org.apache.flink.util.Preconditions;

/** A utility class that provides BLAS routines over matrices and vectors. */
public class BLAS {

    private static final dev.ludovic.netlib.BLAS NATIVE_BLAS =
            dev.ludovic.netlib.BLAS.getInstance();

    /**
     * \sum_i |x_i| .
     *
     * @param x x
     * @return
     */
    public static double asum(DenseVector x) {
        return NATIVE_BLAS.dasum(x.size(), x.values, 0, 1);
    }

    /**
     * y += a * x .
     *
     * @param a a
     * @param x x
     * @param y y
     */
    public static void axpy(double a, DenseVector x, DenseVector y) {
        Preconditions.checkArgument(x.size() == y.size(), "Array dimension mismatched.");
        NATIVE_BLAS.daxpy(x.size(), a, x.values, 1, y.values, 1);
    }

    /**
     * x \cdot y .
     *
     * @param x x
     * @param y y
     * @return x \cdot y
     */
    public static double dot(DenseVector x, DenseVector y) {
        Preconditions.checkArgument(x.size() == y.size(), "Array dimension mismatched.");
        return NATIVE_BLAS.ddot(x.size(), x.values, 1, y.values, 1);
    }

    /**
     * \sum_i x_i * x_i .
     *
     * @param x x
     * @return \sum_i x_i * x_i
     */
    public static double norm2(DenseVector x) {
        return NATIVE_BLAS.dnrm2(x.size(), x.values, 1);
    }

    /**
     * x = x * a .
     *
     * @param a a
     * @param x x
     */
    public static void scal(double a, DenseVector x) {
        NATIVE_BLAS.dscal(x.size(), a, x.values, 1);
    }
}
