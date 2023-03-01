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
import org.apache.flink.ml.common.optimizer.ps.datastorage.DenseLongVector;
import org.apache.flink.ml.common.optimizer.ps.datastorage.StorageUtils;
import org.apache.flink.ml.util.Bits;

/** The indices one worker needs to pull from a ps. */
public class SparsePullModeM implements Message {
    public final int modelId;
    public final int psId;
    public final int workerId;
    public final DenseLongVector pullModelIndices;

    public static final MessageType type = MessageType.SPARSE_PULL_MODEL;

    public SparsePullModeM(int modelId, int psId, int workerId, DenseLongVector pullModelIndices) {
        this.modelId = modelId;
        this.psId = psId;
        this.workerId = workerId;
        this.pullModelIndices = pullModelIndices;
    }

    public static SparsePullModeM readFromBytes(byte[] bytesData, int offset) {
        int modelId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        int psId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        int workerId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        DenseLongVector toPullIndices = StorageUtils.readFromBytes(bytesData, offset);
        return new SparsePullModeM(modelId, psId, workerId, toPullIndices);
    }

    /**
     * writes the instance to bytes array starts from offset.
     *
     * @param buffer
     * @param offset
     * @return the next offset.
     */
    @Override
    public int writeToBytes(byte[] buffer, int offset) {
        Bits.putInt(buffer, offset, this.modelId);
        offset += Integer.BYTES;
        Bits.putInt(buffer, offset, this.psId);
        offset += Integer.BYTES;
        Bits.putInt(buffer, offset, this.workerId);
        offset += Integer.BYTES;
        offset = StorageUtils.writeToBytes(this.pullModelIndices, buffer, offset);
        return offset;
    }

    @Override
    public MessageType getType() {
        return MessageType.SPARSE_PULL_MODEL;
    }

    @Override
    public int getSizeInBytes() {
        return Integer.BYTES * 3 + StorageUtils.getNumBytes(pullModelIndices);
    }
}
