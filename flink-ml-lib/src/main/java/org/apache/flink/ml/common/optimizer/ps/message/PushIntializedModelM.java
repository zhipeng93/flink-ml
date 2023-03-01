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
import org.apache.flink.ml.common.optimizer.ps.RangeModelPartitioner;
import org.apache.flink.ml.common.optimizer.ps.datastorage.StorageUtils;
import org.apache.flink.ml.util.Bits;

/** The initial data pushed from worker to ps. */
public class PushIntializedModelM implements Message {
    public final int modelId;
    public final int psId;
    public final RangeModelPartitioner partitioner;
    // TODO: add model data class for specific model data types. Currently we support DenseVector,
    // SparseVector and Double.
    public final Object modelData;

    public static final MessageType type = MessageType.PUSH_INTIALIZEd_MODEL;

    public PushIntializedModelM(
            int modelId, int psId, RangeModelPartitioner partitioner, Object modelData) {
        this.modelId = modelId;
        this.psId = psId;
        this.partitioner = partitioner;
        this.modelData = modelData;
    }

    public static PushIntializedModelM readFromBytes(byte[] bytesData, int offset) {
        int modelId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        int psId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        RangeModelPartitioner partitioner = RangeModelPartitioner.readFromBytes(bytesData, offset);
        offset += RangeModelPartitioner.getNumBytes();
        Object modelData = StorageUtils.readFromBytes(bytesData, offset);
        return new PushIntializedModelM(modelId, psId, partitioner, modelData);
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
        this.partitioner.writeToBytes(buffer, offset);
        offset += RangeModelPartitioner.getNumBytes();
        offset = StorageUtils.writeToBytes(this.modelData, buffer, offset);
        return offset;
    }

    /**
     * Returns the ps id of this request.
     *
     * @param bytesData
     * @param offset the offset of this message.
     * @return
     */
    public int getPsId(byte[] bytesData, int offset) {
        return Bits.getInt(bytesData, offset + Integer.BYTES);
    }

    @Override
    public MessageType getType() {
        return MessageType.PUSH_INTIALIZEd_MODEL;
    }

    @Override
    public int getSizeInBytes() {
        return Integer.BYTES
                + Integer.BYTES
                + RangeModelPartitioner.getNumBytes()
                + StorageUtils.getNumBytes(modelData);
    }
}
