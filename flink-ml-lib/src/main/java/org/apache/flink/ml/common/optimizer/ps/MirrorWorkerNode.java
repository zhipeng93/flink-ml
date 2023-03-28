package org.apache.flink.ml.common.optimizer.ps;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.optimizer.ps.datastorage.DenseDoubleVectorStorage;
import org.apache.flink.ml.common.optimizer.ps.message.MessageUtils;
import org.apache.flink.ml.common.optimizer.ps.message.PulledModelM;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges the message from different servers by modelId.
 *
 * <p>Note that for each single-thread worker, there are at exactly #numPss pieces for each pull
 * request in the feedback edge.
 */
public class MirrorWorkerNode extends AbstractStreamOperator<byte[]>
        implements OneInputStreamOperator<Tuple2<Integer, byte[]>, byte[]> {

    Map<Integer, List<PulledModelM>> pullsByModel = new HashMap<>();
    int workerId = -1;

    private final int numPss;

    public MirrorWorkerNode(int numPss) {
        this.numPss = numPss;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.workerId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Integer, byte[]>> element) throws Exception {
        Preconditions.checkState(element.getValue().f0 == workerId);
        PulledModelM pulledModelM = MessageUtils.readFromBytes(element.getValue().f1, 0);
        int modelId = pulledModelM.modelId;
        Preconditions.checkState(pulledModelM.workerId == workerId);
        if (!pullsByModel.containsKey(modelId)) {
            pullsByModel.put(modelId, new ArrayList<>(numPss));
        }
        pullsByModel.get(modelId).add(pulledModelM);
        trySendingPulls(modelId, numPss);
    }

    private void trySendingPulls(int modelId, int numPieces) {
        if (pullsByModel.get(modelId).size() == numPieces) {
            List<PulledModelM> pullMessages = pullsByModel.remove(modelId);
            Comparator<PulledModelM> comparator = Comparator.comparingInt(o -> o.psId);
            pullMessages.sort(comparator);

            int size = 0;
            for (PulledModelM pulledModelM : pullMessages) {
                size += pulledModelM.pulledValues.values.length;
            }
            double[] answer = new double[size];
            int offset = 0;
            for (PulledModelM pulledModelM : pullMessages) {
                double[] values = pulledModelM.pulledValues.values;
                System.arraycopy(values, 0, answer, offset, values.length);
                offset += values.length;
            }
            PulledModelM pulledModelM =
                    new PulledModelM(modelId, -1, workerId, new DenseDoubleVectorStorage(answer));
            output.collect(new StreamRecord<>(MessageUtils.toBytes(pulledModelM)));
        }
    }
}
