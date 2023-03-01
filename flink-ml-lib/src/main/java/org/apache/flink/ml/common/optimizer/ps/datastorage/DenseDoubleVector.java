package org.apache.flink.ml.common.optimizer.ps.datastorage;

/** Pulled model values from servers. */
public class DenseDoubleVector {
    public static final PSDataType DATA_TYPE = PSDataType.DENSE_DOUBLE_VECTOR;
    public final double[] values;

    public DenseDoubleVector(double[] values) {
        this.values = values;
    }
}
