package org.apache.flink.ml.common.optimizer.ps.serverstorage;

import org.apache.flink.util.Preconditions;

/** A vector on the server that stores a piece of model data in [startIndex, endIndex). */
public class ServerVector {
    public final double[] data;
    public final long startIndex;
    public final long endIndex;

    public ServerVector(long startIndex, long endIndex, double[] data) {
        Preconditions.checkArgument(endIndex - startIndex < Integer.MAX_VALUE);
        this.data = data;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public double[] getData(long[] indices) {
        double[] values = new double[indices.length];
        for (int i = 0; i < indices.length; i++) {
            values[i] = data[(int) (indices[i] - startIndex)];
        }
        return values;
    }
}
