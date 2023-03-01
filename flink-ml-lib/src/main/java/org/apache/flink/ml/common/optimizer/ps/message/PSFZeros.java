package org.apache.flink.ml.common.optimizer.ps.message;

import org.apache.flink.ml.common.optimizer.ps.MessageType;
import org.apache.flink.ml.util.Bits;

/** Message sent by worker to sever that initialize a dense vector as model data. */
public class PSFZeros implements Message {
    public final int modelId;
    public final int psId;
    public final long startIndex;
    public final long endIndex;

    public PSFZeros(int modelId, int psId, long startIndex, long endIndex) {
        this.modelId = modelId;
        this.psId = psId;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public static PSFZeros readFromBytes(byte[] bytesData, int offset) {
        int modelId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        int psId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        long startIndex = Bits.getLong(bytesData, offset);
        offset += Long.BYTES;
        long endIndex = Bits.getLong(bytesData, offset);
        offset += Long.BYTES;
        return new PSFZeros(modelId, psId, startIndex, endIndex);
    }

    @Override
    public int writeToBytes(byte[] buffer, int offset) {
        Bits.putInt(buffer, offset, this.modelId);
        offset += Integer.BYTES;
        Bits.putInt(buffer, offset, this.psId);
        offset += Integer.BYTES;
        Bits.putLong(buffer, offset, this.startIndex);
        offset += Long.BYTES;
        Bits.putLong(buffer, offset, this.endIndex);
        offset += Long.BYTES;
        return offset;
    }

    @Override
    public MessageType getType() {
        return MessageType.PSF_ZEROS;
    }

    @Override
    public int getSizeInBytes() {
        return Integer.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES;
    }
}
