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

package org.apache.flink.ml.common.optimizer.ps.message;

import org.apache.flink.ml.common.optimizer.ps.MessageType;
import org.apache.flink.ml.common.optimizer.ps.datastorage.SparseLongDoubleVector;
import org.apache.flink.ml.common.optimizer.ps.datastorage.StorageUtils;
import org.apache.flink.ml.util.Bits;

/** The gradient to push to ps. */
public class PushGradM implements Message {
    public final int modelId;
    public final int psId;
    public final SparseLongDoubleVector grad;
    public final double weight;

    public PushGradM(int modelId, int psId, SparseLongDoubleVector grad, double weight) {
        this.modelId = modelId;
        this.psId = psId;
        this.grad = grad;
        this.weight = weight;
    }

    public static PushGradM readFromBytes(byte[] bytesData, int offset) {
        int modelId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        int psId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        double weight = Bits.getDouble(bytesData, offset);
        offset += Double.BYTES;
        SparseLongDoubleVector modelData = StorageUtils.readFromBytes(bytesData, offset);
        return new PushGradM(modelId, psId, modelData, weight);
    }

    @Override
    public int writeToBytes(byte[] buffer, int offset) {
        Bits.putInt(buffer, offset, this.modelId);
        offset += Integer.BYTES;
        Bits.putInt(buffer, offset, this.psId);
        offset += Integer.BYTES;
        Bits.putDouble(buffer, offset, this.weight);
        offset += Double.BYTES;
        offset = StorageUtils.writeToBytes(this.grad, buffer, offset);
        return offset;
    }

    @Override
    public MessageType getType() {
        return MessageType.PUSH_GRAD;
    }

    @Override
    public int getSizeInBytes() {
        return Integer.BYTES + Integer.BYTES + Double.BYTES + StorageUtils.getNumBytes(grad);
    }
}
