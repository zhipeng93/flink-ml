package org.apache.flink.ml.common.optimizer.ps.datastorage;

/** Indices to pull model parameter from servers. */
public class DenseLongVector {
    public static final PSDataType DATA_TYPE = PSDataType.DENSE_LONG_VECTOR;

    public final long[] values;

    public DenseLongVector(long[] values) {
        this.values = values;
    }
}
