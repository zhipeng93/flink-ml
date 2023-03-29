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

package org.apache.flink.ml.common.optimizer;

import org.apache.flink.ml.common.optimizer.ps.datastorage.StorageUtils;
import org.apache.flink.ml.linalg.SparseLongDoubleVector;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests {@link StorageUtils}. */
public class StorageUtilsTest {

    @Test
    public void testDenseLongVector() {
        long[] values = new long[] {1L, 2L, 4L};
        byte[] bytes = new byte[32];
        StorageUtils.writeToBytes(values, bytes, 2);
        long[] returnedDenseLongVector = StorageUtils.readFromBytes(bytes, 2);
        assertArrayEquals(returnedDenseLongVector, values);
        assertEquals(30, StorageUtils.getNumBytes(values));
    }

    @Test
    public void testSparseLongDoubleVector() {
        SparseLongDoubleVector sparseLongDoubleVector =
                new SparseLongDoubleVector(10L, new long[] {1, 2, 9}, new double[] {1.1, 1.2, 1.3});
        byte[] bytes = new byte[64];
        StorageUtils.writeToBytes(sparseLongDoubleVector, bytes, 2);
        SparseLongDoubleVector returnedSparseLongDoubleVector =
                StorageUtils.readFromBytes(bytes, 2);
        assertArrayEquals(sparseLongDoubleVector.indices, returnedSparseLongDoubleVector.indices);
        assertArrayEquals(
                sparseLongDoubleVector.values, returnedSparseLongDoubleVector.values, 1e-7);
        assertEquals(sparseLongDoubleVector.size, returnedSparseLongDoubleVector.size);
        assertEquals(62, StorageUtils.getNumBytes(returnedSparseLongDoubleVector));
    }

    @Test
    public void testDenseDoubleVector() {
        double[] values = new double[] {1.1, 1.2, 1.3};
        byte[] bytes = new byte[32];
        StorageUtils.writeToBytes(values, bytes, 2);
        double[] returnedDenseVector = StorageUtils.readFromBytes(bytes, 2);
        assertArrayEquals(values, returnedDenseVector, 1e-7);
        assertEquals(30, StorageUtils.getNumBytes(values));
    }

    @Test
    public void testDouble() {
        double value = 1.1;
        byte[] bytes = new byte[12];
        StorageUtils.writeToBytes(value, bytes, 2);
        double returnedDouble = StorageUtils.readFromBytes(bytes, 2);
        assertEquals(returnedDouble, value, 1e-9);
        assertEquals(10, StorageUtils.getNumBytes(value));
    }
}
