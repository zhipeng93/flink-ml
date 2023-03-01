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
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests {@link SparseLongDoubleVector}. */
public class SparseLoneDoubleVectorTest {

    @Test
    public void testSerializer() throws IOException {
        long n = 4;
        long[] indices = new long[] {0, 2, 3};
        double[] values = new double[] {0.1, 0.3, 0.4};
        SparseLongDoubleVector vector = new SparseLongDoubleVector(n, indices, values);
        SparseLongDoubleVectorSerializer serializer = SparseLongDoubleVectorSerializer.INSTANCE;

        ByteArrayOutputStream bOutput = new ByteArrayOutputStream(1024);
        DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(bOutput);
        serializer.serialize(vector, output);

        byte[] b = bOutput.toByteArray();
        ByteArrayInputStream bInput = new ByteArrayInputStream(b);
        DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(bInput);
        SparseLongDoubleVector vector2 = serializer.deserialize(input);

        assertEquals(vector.size, vector2.size);
        assertArrayEquals(vector.indices, vector2.indices);
        assertArrayEquals(vector.values, vector2.values, 1e-5);
    }
}
