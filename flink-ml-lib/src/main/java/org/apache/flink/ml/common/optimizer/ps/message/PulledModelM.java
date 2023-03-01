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
import org.apache.flink.ml.common.optimizer.ps.datastorage.DenseDoubleVectorStorage;
import org.apache.flink.ml.common.optimizer.ps.datastorage.StorageUtils;
import org.apache.flink.ml.util.Bits;

/** The model data pulled from ps. */
public class PulledModelM implements Message {
    public final int modelId;
    public final int workerId;

    public final int psId;
    public final DenseDoubleVectorStorage pulledValues;

    public PulledModelM(
            int modelId, int psId, int workerId, DenseDoubleVectorStorage pulledValues) {
        this.modelId = modelId;
        this.psId = psId;
        this.workerId = workerId;
        this.pulledValues = pulledValues;
    }

    public static PulledModelM readFromBytes(byte[] bytesData, int offset) {
        int modelId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        int psId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        int workerId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        DenseDoubleVectorStorage modelData = StorageUtils.readFromBytes(bytesData, offset);
        return new PulledModelM(modelId, psId, workerId, modelData);
    }

    @Override
    public int writeToBytes(byte[] buffer, int offset) {
        Bits.putInt(buffer, offset, this.modelId);
        offset += Integer.BYTES;
        Bits.putInt(buffer, offset, this.psId);
        offset += Integer.BYTES;
        Bits.putInt(buffer, offset, this.workerId);
        offset += Integer.BYTES;
        offset = StorageUtils.writeToBytes(this.pulledValues, buffer, offset);
        return offset;
    }

    @Override
    public MessageType getType() {
        return MessageType.PULLED_MODEL;
    }

    @Override
    public int getSizeInBytes() {
        return Integer.BYTES
                + Integer.BYTES
                + Integer.BYTES
                + StorageUtils.getNumBytes(pulledValues);
    }
}
