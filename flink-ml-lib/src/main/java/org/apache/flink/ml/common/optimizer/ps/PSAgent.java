package org.apache.flink.ml.common.optimizer.ps;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.optimizer.ps.message.Message;
import org.apache.flink.ml.common.optimizer.ps.message.MessageUtils;
import org.apache.flink.ml.common.optimizer.ps.message.PSFZeros;
import org.apache.flink.ml.common.optimizer.ps.message.PushGradM;
import org.apache.flink.ml.common.optimizer.ps.message.PushIntializedModelM;
import org.apache.flink.ml.common.optimizer.ps.message.SparsePullModeM;
import org.apache.flink.ml.linalg.SparseLongDoubleVector;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Agent for workers to talk with servers. */
public class PSAgent {
    public final int workerId;
    // modelId to partitioner map.
    public final Map<Integer, RangeModelPartitioner> partitioners;
    Output<StreamRecord<Tuple2<Integer, byte[]>>> output;

    StreamRecord<Tuple2<Integer, byte[]>> reused;

    public PSAgent(int workerId, Output<StreamRecord<Tuple2<Integer, byte[]>>> output) {
        this.workerId = workerId;
        this.output = output;
        this.partitioners = new HashMap<>();
        reused = new StreamRecord<>(null);
    }

    public void addPartitioner(int modelId, RangeModelPartitioner partitioner) {
        Preconditions.checkState(!partitioners.containsKey(modelId));
        partitioners.put(modelId, partitioner);
    }

    // Pushes the sparse gradient to servers.
    public void sparsePushGradient(int modelId, SparseLongDoubleVector vector, double weight) {
        long size = vector.size;
        long[] indices = vector.indices;
        double[] values = vector.values;

        RangeModelPartitioner partitioner = partitioners.get(modelId);
        Preconditions.checkState(partitioner != null);

        int s = 0;
        for (int psId = 0; psId < partitioner.numPss; psId++) {
            Tuple2<Long, Long> startAndEnd = partitioner.getStartAndEnd(psId);
            int e = s;
            while (e < indices.length && indices[e] < startAndEnd.f1) {
                e++;
            }

            // Also pushes the empty message for atomic of push/pull in async setting.
            long[] splitIndices = new long[0];
            double[] splitValues = new double[0];
            if (s < e) {
                splitIndices = Arrays.copyOfRange(indices, s, e);
                splitValues = Arrays.copyOfRange(values, s, e);
            }
            s = e;
            Message message =
                    new PushGradM(
                            modelId,
                            workerId,
                            psId,
                            new SparseLongDoubleVector(size, splitIndices, splitValues),
                            weight);
            output.collect(new StreamRecord<>(Tuple2.of(psId, MessageUtils.toBytes(message))));
        }
    }

    // Push initialized model data to servers.
    // TODO: Add incremental training.
    public void push(PushIntializedModelM pushIntializedModelM) {}

    public void zeros(int modelId, long dim) {
        RangeModelPartitioner partitioner = partitioners.get(modelId);
        Preconditions.checkState(partitioner != null);

        for (int psId = 0; psId < partitioner.numPss; psId++) {
            Tuple2<Long, Long> startAndEnd = partitioner.getStartAndEnd(psId);
            PSFZeros psfZeros = new PSFZeros(modelId, psId, startAndEnd.f0, startAndEnd.f1);
            output.collect(new StreamRecord<>(Tuple2.of(psId, MessageUtils.toBytes(psfZeros))));
        }
    }

    // Pulls some dimensions of the model data.
    public void sparsePullModel(int modelId, long[] indices) {
        RangeModelPartitioner partitioner = partitioners.get(modelId);
        Preconditions.checkState(partitioner != null);

        int s = 0;
        for (int psId = 0; psId < partitioner.numPss; psId++) {
            Tuple2<Long, Long> startAndEnd = partitioner.getStartAndEnd(psId);
            int e = s;
            while (e < indices.length && indices[e] < startAndEnd.f1) {
                e++;
            }

            long[] splitIndices = new long[0];
            if (s < e) {
                splitIndices = Arrays.copyOfRange(indices, s, e);
            }
            s = e;
            Message message = new SparsePullModeM(modelId, psId, workerId, splitIndices);
            output.collect(new StreamRecord<>(Tuple2.of(psId, MessageUtils.toBytes(message))));
        }
    }
}
