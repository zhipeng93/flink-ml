package org.apache.flink.ml.common.optimizer.ps.datastorage;

import org.apache.flink.util.Preconditions;

/** Used to encode sparse gradients to push to servers. */
public class SparseLongDoubleVector {
    public static final PSDataType DATA_TYPE = PSDataType.SPARSE_LONG_DOUBLE_VECTOR;
    public final long size;
    public final long[] indices;
    public final double[] values;

    public SparseLongDoubleVector(long size, long[] indices, double[] values) {
        Preconditions.checkState(indices.length == values.length);
        Preconditions.checkState(size < 0 || size > indices[indices.length - 1]);
        this.size = size;
        this.indices = indices;
        this.values = values;
    }
}
