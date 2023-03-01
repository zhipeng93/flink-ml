package org.apache.flink.ml.common.feature;

import org.apache.flink.ml.linalg.SparseLongDoubleVector;

/** A data point to represent values that use long as index and double as values. */
public class LabeledLargePointWithWeight {
    public SparseLongDoubleVector features;

    public double label;

    public double weight;

    public LabeledLargePointWithWeight(
            SparseLongDoubleVector features, double label, double weight) {
        this.features = features;
        this.label = label;
        this.weight = weight;
    }

    /** Makes it pojo to use flink serializer. */
    public LabeledLargePointWithWeight() {}
}
