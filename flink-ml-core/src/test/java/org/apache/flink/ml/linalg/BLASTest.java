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

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests the {@link BLAS}. */
public class BLASTest {

    private static final double TOL = 1.0e-6;

    private DenseVector denseVecInput = new DenseVector(new double[] {1, -2, 3, 4, -5});

    @Test
    public void testAsum() {
        assertEquals(BLAS.asum(denseVecInput), 15, TOL);
    }

    @Test
    public void testAxpy() {
        DenseVector yDenseVec = new DenseVector(new double[] {1, 2, 3, 4, 5});
        BLAS.axpy(1, denseVecInput, yDenseVec);
        assertArrayEquals(yDenseVec.values, new double[] {2, 0, 6, 8, 0}, TOL);
    }

    @Test
    public void testDot() {
        assertEquals(
                BLAS.dot(denseVecInput, new DenseVector(new double[] {1, 2, 3, 4, 5})), -3, TOL);
    }

    @Test
    public void testNorm2() {
        assertEquals(BLAS.norm2(denseVecInput), Math.sqrt(55), TOL);
    }

    @Test
    public void testScal() {
        BLAS.scal(2, denseVecInput);
        assertArrayEquals(denseVecInput.values, new double[] {2, -4, 6, 8, -10}, TOL);
    }
}
