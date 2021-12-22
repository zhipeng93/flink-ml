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
    /** For level-1 function dspmv, use javaBLAS for better performance. */
    private static final dev.ludovic.netlib.BLAS JAVA_BLAS =
            dev.ludovic.netlib.JavaBLAS.getInstance();

    /** \sum_i |x_i| . */
    public static double asum(Vector x) {
        if (x instanceof DenseVector) {
            return asum((DenseVector) x);
        } else if (x instanceof SparseVector) {
            return asum((SparseVector) x);
        }
        throw new RuntimeException("Unsupported vector type.");
    }

    /** y += a * x . */
    public static void axpy(double a, Vector x, Vector y) {
        Preconditions.checkArgument(x.size() == y.size(), "Vector size mismatched.");
        if (y instanceof DenseVector) {
            if (x instanceof SparseVector) {
                axpy(a, (SparseVector) x, (DenseVector) y);
            } else if (x instanceof DenseVector) {
                axpy(a, (DenseVector) x, (DenseVector) y);
            } else {
                throw new RuntimeException("Unsupported vector type.");
            }
        } else {
            throw new RuntimeException("Axpy only supports adding to a dense vector.");
        }
    }

    /** x \cdot y . */
    public static double dot(Vector x, Vector y) {
        Preconditions.checkArgument(x.size() == y.size(), "Vector size mismatched.");
        if (x instanceof DenseVector) {
            if (y instanceof DenseVector) {
                return dot((DenseVector) x, (DenseVector) y);
            } else if (y instanceof SparseVector) {
                return dot((SparseVector) y, (DenseVector) x);
            } else {
                throw new RuntimeException("Unsupported vector type.");
            }
        } else {
            if (y instanceof DenseVector) {
                return dot((SparseVector) x, (DenseVector) y);
            } else if (y instanceof SparseVector) {
                return dot((SparseVector) x, (SparseVector) y);
            } else {
                throw new RuntimeException("Unsupported vector type.");
            }
        }
    }

    /** \sqrt(\sum_i x_i * x_i) . */
    public static double norm2(Vector x) {
        if (x instanceof SparseVector) {
            return norm2((SparseVector) x);
        } else if (x instanceof DenseVector) {
            return norm2((DenseVector) x);
        } else {
            throw new RuntimeException("Unsupported vector type.");
        }
    }

    /** x = x * a . */
    public static void scal(double a, Vector x) {
        if (x instanceof SparseVector) {
            scal(a, (SparseVector) x);
        } else if (x instanceof DenseVector) {
            scal(a, (DenseVector) x);
        } else {
            throw new RuntimeException("Unsupported vector type.");
        }
    }

    /**
     * y = alpha * matrix * x + beta * y or y = alpha * (matrix^T) * x + beta * y.
     *
     * @param alpha The alpha value.
     * @param matrix Dense matrix with size m x n.
     * @param transMatrix Whether transposes matrix before multiply.
     * @param x Dense vector with size n.
     * @param beta The beta value.
     * @param y Dense vector with size m.
     */
    public static void gemv(
            double alpha,
            DenseMatrix matrix,
            boolean transMatrix,
            DenseVector x,
            double beta,
            DenseVector y) {
        Preconditions.checkArgument(
                transMatrix
                        ? (matrix.numRows() == x.size() && matrix.numCols() == y.size())
                        : (matrix.numRows() == y.size() && matrix.numCols() == x.size()),
                "Matrix and vector size mismatched.");
        final String trans = transMatrix ? "T" : "N";
        JAVA_BLAS.dgemv(
                trans,
                matrix.numRows(),
                matrix.numCols(),
                alpha,
                matrix.values,
                matrix.numRows(),
                x.values,
                1,
                beta,
                y.values,
                1);
    }

    private static double asum(DenseVector x) {
        return JAVA_BLAS.dasum(x.size(), x.values, 0, 1);
    }

    private static double asum(SparseVector x) {
        double sum = 0;
        for (double val : x.values) {
            sum += Math.abs(val);
        }
        return sum;
    }

    private static void axpy(double a, DenseVector x, DenseVector y) {
        JAVA_BLAS.daxpy(x.size(), a, x.values, 1, y.values, 1);
    }

    private static void axpy(double a, SparseVector x, DenseVector y) {
        for (int i = 0; i < x.indices.length; i++) {
            int index = x.indices[i];
            y.values[index] += a * x.values[i];
        }
    }

    private static double dot(DenseVector x, DenseVector y) {
        return JAVA_BLAS.ddot(x.size(), x.values, 1, y.values, 1);
    }

    private static double dot(SparseVector x, DenseVector y) {
        double dot = 0;
        for (int i = 0; i < x.indices.length; i++) {
            int index = x.indices[i];
            dot += y.values[index] * x.values[i];
        }
        return dot;
    }

    private static double dot(SparseVector x, SparseVector y) {
        double dot = 0;
        int sidx = 0;
        int sidy = 0;
        while (sidx < x.indices.length && sidy < y.indices.length) {
            int indexX = x.indices[sidx];
            while (sidy < y.indices.length && y.indices[sidy] < indexX) {
                sidy++;
            }
            if (sidy < y.indices.length && y.indices[sidy] == indexX) {
                dot += x.values[sidx] * y.values[sidy];
                sidy += 1;
            }
            sidx += 1;
        }
        return dot;
    }

    private static double norm2(DenseVector x) {
        return JAVA_BLAS.dnrm2(x.size(), x.values, 1);
    }

    private static double norm2(SparseVector x) {
        return JAVA_BLAS.dnrm2(x.values.length, x.values, 1);
    }

    private static void scal(double a, DenseVector x) {
        JAVA_BLAS.dscal(x.size(), a, x.values, 1);
    }

    private static void scal(double a, SparseVector x) {
        JAVA_BLAS.dscal(x.values.length, a, x.values, 1);
    }
}
