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

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.ml.linalg.typeinfo.SparseLongDoubleVectorSerializer;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests {@link SparseLongDoubleVector}. */
public class SparseLongDoubleVectorTest {
    private static final double TOLERANCE = 1e-7;

    @Test
    public void testConstructor() {
        long n = 4;
        long[] indices = new long[] {0, 2, 3};
        double[] values = new double[] {0.1, 0.3, 0.4};

        SparseLongDoubleVector vector = Vectors.sparse(n, indices, values);
        assertEquals(n, vector.n);
        assertArrayEquals(indices, vector.indices);
        assertArrayEquals(values, vector.values, 1e-5);
        assertEquals("(4, [0, 2, 3], [0.1, 0.3, 0.4])", vector.toString());
    }

    @Test
    public void testDuplicateIndex() {
        long n = 4;
        long[] indices = new long[] {0, 2, 2};
        double[] values = new double[] {0.1, 0.3, 0.4};

        try {
            Vectors.sparse(n, indices, values);
            Assert.fail("Expected IllegalArgumentException.");
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Indices duplicated.", e.getMessage());
        }
    }

    @Test
    public void testAllZeroVector() {
        long n = 4;
        SparseLongDoubleVector vector = Vectors.sparse(n, new long[0], new double[0]);
        try {
            vector.toArray();
        } catch (Exception e) {
            assertEquals(UnsupportedOperationException.class, e.getClass());
            assertEquals("LongDoubleVector cannot be converted to dense array.", e.getMessage());
        }
    }

    @Test
    public void testUnsortedIndex() {
        SparseLongDoubleVector vector;

        vector = Vectors.sparse(4, new long[] {2}, new double[] {0.3});
        assertEquals(4, vector.n);
        assertArrayEquals(new long[] {2}, vector.indices);
        assertArrayEquals(new double[] {0.3}, vector.values, 1e-5);

        vector = Vectors.sparse(4, new long[] {1, 2}, new double[] {0.2, 0.3});
        assertEquals(4, vector.n);
        assertArrayEquals(new long[] {1, 2}, vector.indices);
        assertArrayEquals(new double[] {0.2, 0.3}, vector.values, 1e-5);

        vector = Vectors.sparse(4, new long[] {2, 1}, new double[] {0.3, 0.2});
        assertEquals(4, vector.n);
        assertArrayEquals(new long[] {1, 2}, vector.indices);
        assertArrayEquals(new double[] {0.2, 0.3}, vector.values, 1e-5);

        vector = Vectors.sparse(4, new long[] {3, 2, 0}, new double[] {0.4, 0.3, 0.1});
        assertEquals(4, vector.n);
        assertArrayEquals(new long[] {0, 2, 3}, vector.indices);
        assertArrayEquals(new double[] {0.1, 0.3, 0.4}, vector.values, 1e-5);

        vector = Vectors.sparse(4, new long[] {2, 0, 3}, new double[] {0.3, 0.1, 0.4});
        assertEquals(4, vector.n);
        assertArrayEquals(new long[] {0, 2, 3}, vector.indices);
        assertArrayEquals(new double[] {0.1, 0.3, 0.4}, vector.values, 1e-5);

        vector =
                Vectors.sparse(
                        7,
                        new long[] {6, 5, 4, 3, 2, 1, 0},
                        new double[] {0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1});
        assertEquals(7, vector.n);
        assertArrayEquals(new long[] {0, 1, 2, 3, 4, 5, 6}, vector.indices);
        assertArrayEquals(new double[] {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7}, vector.values, 1e-5);
    }

    @Test
    public void testSerializer() throws IOException {
        long n = 4;
        long[] indices = new long[] {0, 2, 3};
        double[] values = new double[] {0.1, 0.3, 0.4};
        SparseLongDoubleVector vector = Vectors.sparse(n, indices, values);
        SparseLongDoubleVectorSerializer serializer = SparseLongDoubleVectorSerializer.INSTANCE;

        ByteArrayOutputStream bOutput = new ByteArrayOutputStream(1024);
        DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(bOutput);
        serializer.serialize(vector, output);

        byte[] b = bOutput.toByteArray();
        ByteArrayInputStream bInput = new ByteArrayInputStream(b);
        DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(bInput);
        SparseLongDoubleVector vector2 = serializer.deserialize(input);

        assertEquals(vector.n, vector2.n);
        assertArrayEquals(vector.indices, vector2.indices);
        assertArrayEquals(vector.values, vector2.values, 1e-5);
    }

    @Test
    public void testClone() {
        SparseLongDoubleVector sparseVec =
                Vectors.sparse(3, new long[] {0, 2}, new double[] {1, 3});
        SparseLongDoubleVector clonedSparseVec = sparseVec.clone();
        assertEquals(3, clonedSparseVec.size().intValue());
        assertArrayEquals(clonedSparseVec.indices, new long[] {0, 2});
        assertArrayEquals(clonedSparseVec.values, new double[] {1, 3}, TOLERANCE);

        clonedSparseVec.values[0] = -1;
        assertEquals(sparseVec.size(), clonedSparseVec.size());
        assertArrayEquals(sparseVec.indices, new long[] {0, 2});
        assertArrayEquals(sparseVec.values, new double[] {1, 3}, TOLERANCE);
        assertArrayEquals(clonedSparseVec.indices, new long[] {0, 2});
        assertArrayEquals(clonedSparseVec.values, new double[] {-1, 3}, TOLERANCE);
    }

    @Test
    public void testGetAndSet() {
        SparseLongDoubleVector sparseVec = Vectors.sparse(4, new long[] {2}, new double[] {0.3});
        assertEquals(0, sparseVec.get(0L), TOLERANCE);
        assertEquals(0.3, sparseVec.get(2L), TOLERANCE);

        sparseVec.set(2L, 0.5);
        assertEquals(0.5, sparseVec.get(2L), TOLERANCE);

        sparseVec.set(0L, 0.1);
        assertEquals(0.1, sparseVec.get(0L), TOLERANCE);
    }
}
