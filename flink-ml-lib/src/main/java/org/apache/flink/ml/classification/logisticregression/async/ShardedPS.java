package org.apache.flink.ml.classification.logisticregression.async;

import org.apache.flink.streaming.api.datastream.DataStream;

/** Utility class to provides access to model parameters. */
public class ShardedPS {

    public static DataStream<double[]> pullModel(
            DataStream<long[]> indices, ShardedModel shardedModel) {
        return null;
    }
}
