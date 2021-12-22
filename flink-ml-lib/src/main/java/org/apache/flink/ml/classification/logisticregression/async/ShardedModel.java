package org.apache.flink.ml.classification.logisticregression.async;

import org.apache.flink.streaming.api.datastream.DataStream;

public class ShardedModel {

    public final ModelPartitioner modelPartitioner;
    private final DataStream<double[]> partitionedModelData;

    public ShardedModel(
            ModelPartitioner modelPartitioner, DataStream<double[]> partitionedModelData) {
        this.modelPartitioner = modelPartitioner;
        this.partitionedModelData = partitionedModelData;
    }
}
