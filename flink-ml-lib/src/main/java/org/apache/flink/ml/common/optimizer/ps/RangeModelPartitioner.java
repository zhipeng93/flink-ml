package org.apache.flink.ml.common.optimizer.ps;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.util.Bits;
import org.apache.flink.util.Preconditions;

/** Range model partitioner for vector. */
public class RangeModelPartitioner {
    public long dim;
    public int numPss;

    public int modelId;

    public RangeModelPartitioner(long dim, int numPss, int modelId) {
        Preconditions.checkArgument(dim > 0 && numPss > 0 && modelId >= 0);
        this.dim = dim;
        this.numPss = numPss;
        this.modelId = modelId;
    }

    public RangeModelPartitioner() {}

    public Tuple2<Long, Long> getStartAndEnd(int psId) {
        long shardSize = dim / numPss;
        long start = shardSize * psId;
        long end = Math.min(start + shardSize, dim);
        return Tuple2.of(start, end);
    }

    public static int getNumBytes() {
        return Long.BYTES + Integer.BYTES + Integer.BYTES;
    }

    public int writeToBytes(byte[] bytesData, int offset) {
        Bits.putLong(bytesData, offset, dim);
        offset += Long.BYTES;
        Bits.putInt(bytesData, offset, numPss);
        offset += Integer.BYTES;
        Bits.putInt(bytesData, offset, modelId);
        offset += Integer.BYTES;
        return offset;
    }

    public byte[] toBytes() {
        byte[] buffer = new byte[getNumBytes()];
        writeToBytes(buffer, 0);
        return buffer;
    }

    public static RangeModelPartitioner readFromBytes(byte[] bytesData, int offset) {
        long dim = Bits.getLong(bytesData, offset);
        offset += Long.BYTES;
        int numPss = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        int modelId = Bits.getInt(bytesData, offset);
        offset += Integer.BYTES;
        return new RangeModelPartitioner(dim, numPss, modelId);
    }
}
