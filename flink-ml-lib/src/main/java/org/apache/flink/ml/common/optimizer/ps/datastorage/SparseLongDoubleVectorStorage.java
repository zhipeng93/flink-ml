package org.apache.flink.ml.common.optimizer.ps.datastorage;

/** Used to encode sparse gradients to push to servers. */
public class SparseLongDoubleVectorStorage {
    public static final PSDataType DATA_TYPE = PSDataType.SPARSE_LONG_DOUBLE_VECTOR;
    public final long size;
    public final long[] indices;
    public final double[] values;

    public SparseLongDoubleVectorStorage(long size, long[] indices, double[] values) {
        // Preconditions.checkState(indices.length == values.length);
        // if (size > 0 && indices.length != 0) {
        //    Preconditions.checkState(size > indices[indices.length - 1]);
        // }

        this.size = size;
        this.indices = indices;
        this.values = values;
    }
}
