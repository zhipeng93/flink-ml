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

package org.apache.flink.ml.common.optimizer.ps.datastorage;

import org.apache.flink.ml.util.Bits;
import org.apache.flink.util.Preconditions;

/** Utility class for parsing data into/from bytes. */
public class StorageUtils {
    public static <T> T readFromBytes(byte[] bytesData, int offset) {
        PSDataType type = PSDataType.valueOf(Bits.getChar(bytesData, offset));
        offset += Character.BYTES;
        switch (type) {
            case DENSE_LONG_VECTOR:
                {
                    int size = Bits.getInt(bytesData, offset);
                    offset += Integer.BYTES;
                    long[] values = new long[size];
                    for (int i = 0; i < size; i++) {
                        values[i] = Bits.getLong(bytesData, offset);
                        offset += Long.BYTES;
                    }
                    return (T) new DenseLongVectorStorage(values);
                }
            case SPARSE_LONG_DOUBLE_VECTOR:
                {
                    long size = Bits.getLong(bytesData, offset);
                    offset += Long.BYTES;
                    int nnz = Bits.getInt(bytesData, offset);
                    offset += Integer.BYTES;
                    long[] indices = new long[nnz];
                    double[] values = new double[nnz];
                    for (int i = 0; i < nnz; i++) {
                        indices[i] = Bits.getLong(bytesData, offset);
                        offset += Long.BYTES;
                        values[i] = Bits.getDouble(bytesData, offset);
                        offset += Double.BYTES;
                    }
                    return (T) new SparseLongDoubleVectorStorage(size, indices, values);
                }
            case DENSE_DOUBLE_VECTOR:
                int size = Bits.getInt(bytesData, offset);
                offset += Integer.BYTES;
                double[] values = new double[size];
                for (int i = 0; i < size; i++) {
                    values[i] = Bits.getDouble(bytesData, offset);
                    offset += Long.BYTES;
                }
                return (T) new DenseDoubleVectorStorage(values);

            case DOUBLE:
                return (T) (Double) Bits.getDouble(bytesData, offset);
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Converts model data to byte array. TODO: We should have a buffer management mechanism.
     *
     * @param modelData
     * @return
     * @param <T>
     */
    public static <T> byte[] toBytesArray(T modelData) {
        int numBytes = StorageUtils.getNumBytes(modelData);
        byte[] buffer = new byte[numBytes];
        int offset = StorageUtils.writeToBytes(modelData, buffer, 0);
        Preconditions.checkState(numBytes == offset);
        return buffer;
    }

    /**
     * Seriaize the model data to bytesData starting from offset.
     *
     * @param modelData
     * @param bytesData
     * @param offset
     * @return The next offset to put elements in.
     * @param <T>
     */
    public static <T> int writeToBytes(T modelData, byte[] bytesData, int offset) {
        if (modelData instanceof DenseLongVectorStorage) {
            DenseLongVectorStorage denseLongVector = (DenseLongVectorStorage) modelData;
            Bits.putChar(bytesData, offset, PSDataType.DENSE_LONG_VECTOR.type);
            offset += Character.BYTES;
            long[] values = denseLongVector.values;
            Bits.putInt(bytesData, offset, values.length);
            offset += Integer.BYTES;
            for (int i = 0; i < values.length; i++) {
                Bits.putLong(bytesData, offset, values[i]);
                offset += Long.BYTES;
            }
        } else if (modelData instanceof SparseLongDoubleVectorStorage) {
            SparseLongDoubleVectorStorage sparseLongDoubleVector =
                    (SparseLongDoubleVectorStorage) modelData;
            Bits.putChar(bytesData, offset, PSDataType.SPARSE_LONG_DOUBLE_VECTOR.type);
            offset += Character.BYTES;

            long[] indices = sparseLongDoubleVector.indices;
            double[] values = sparseLongDoubleVector.values;
            Bits.putLong(bytesData, offset, sparseLongDoubleVector.size);
            offset += Long.BYTES;
            Bits.putInt(bytesData, offset, indices.length);
            offset += Integer.BYTES;
            for (int i = 0; i < indices.length; i++) {
                Bits.putLong(bytesData, offset, indices[i]);
                offset += Long.BYTES;
                Bits.putDouble(bytesData, offset, values[i]);
                offset += Double.BYTES;
            }
        } else if (modelData instanceof DenseDoubleVectorStorage) {
            DenseDoubleVectorStorage denseDoubleVector = (DenseDoubleVectorStorage) modelData;
            Bits.putChar(bytesData, offset, PSDataType.DENSE_DOUBLE_VECTOR.type);
            offset += Character.BYTES;
            double[] values = denseDoubleVector.values;
            Bits.putInt(bytesData, offset, values.length);
            offset += Integer.BYTES;
            for (int i = 0; i < values.length; i++) {
                Bits.putDouble(bytesData, offset, values[i]);
                offset += Long.BYTES;
            }
        } else if (modelData instanceof Double) {
            Double d = (Double) modelData;
            Bits.putChar(bytesData, offset, PSDataType.DOUBLE.type);
            offset += Character.BYTES;
            Bits.putDouble(bytesData, offset, d);
            offset += Double.BYTES;
        } else {
            throw new UnsupportedOperationException();
        }
        return offset;
    }

    /**
     * Get bytes of model data.
     *
     * @param modelData
     * @return
     * @param <T>
     */
    public static <T> int getNumBytes(T modelData) {
        if (modelData instanceof DenseLongVectorStorage) {
            return Character.BYTES
                    + Integer.BYTES
                    + ((DenseLongVectorStorage) modelData).values.length * Long.BYTES;
        } else if (modelData instanceof SparseLongDoubleVectorStorage) {
            return Character.BYTES
                    + Long.BYTES
                    + Integer.BYTES
                    + ((SparseLongDoubleVectorStorage) modelData).indices.length
                            * (Long.BYTES + Double.BYTES);
        } else if (modelData instanceof DenseDoubleVectorStorage) {
            return Character.BYTES
                    + Integer.BYTES
                    + ((DenseDoubleVectorStorage) modelData).values.length * Double.BYTES;
        } else if (modelData instanceof Double) {
            return Character.BYTES + Double.BYTES;
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
