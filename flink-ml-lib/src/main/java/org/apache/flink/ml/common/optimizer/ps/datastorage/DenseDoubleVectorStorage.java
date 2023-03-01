package org.apache.flink.ml.common.optimizer.ps.datastorage;

/** Pulled model values from servers. */
public class DenseDoubleVectorStorage {
    public static final PSDataType DATA_TYPE = PSDataType.DENSE_DOUBLE_VECTOR;
    public final double[] values;

    public DenseDoubleVectorStorage(double[] values) {
        this.values = values;
    }
}
