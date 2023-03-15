package org.apache.flink.ml.common.optimizer.ps;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.ml.common.optimizer.ps.datastorage.DenseDoubleVectorStorage;
import org.apache.flink.ml.common.optimizer.ps.datastorage.SparseLongDoubleVectorStorage;
import org.apache.flink.ml.common.optimizer.ps.message.Message;
import org.apache.flink.ml.common.optimizer.ps.message.MessageUtils;
import org.apache.flink.ml.common.optimizer.ps.message.PSFZeros;
import org.apache.flink.ml.common.optimizer.ps.message.PulledModelM;
import org.apache.flink.ml.common.optimizer.ps.message.PushGradM;
import org.apache.flink.ml.common.optimizer.ps.message.SparsePullModeM;
import org.apache.flink.ml.common.optimizer.ps.serverstorage.ServerVector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** The server node that maintains the model parameters. */
public class ServerNode extends AbstractStreamOperator<Tuple2<Integer, byte[]>>
        implements OneInputStreamOperator<Tuple2<Integer, byte[]>, Tuple2<Integer, byte[]>>,
                IterationListener<Tuple2<Integer, byte[]>> {

    // model id, start index, end index, dense data
    private final OutputTag<Tuple4<Integer, Long, Long, double[]>> modelOutputTag;
    private final int numWorkers;

    private final double learningRate;

    private final Map<Integer, ServerVector> modelData;

    private int epochWatermark = -1;

    int psId = -1;

    // TODO: Store it in state.
    private Map<Integer, Integer> pushRequestsNumReceivedByModelId = new HashMap<>();
    // TODO: Store it in state.
    private Map<Integer, Map<Long, Double>> sparseGradsByModelId = new HashMap<>();
    private Map<Integer, Double> weightByModelId = new HashMap<>();

    private Map<Integer, List<Message>> pendingPullRpcByModelId = new HashMap<>();

    public ServerNode(
            double learningRate,
            int numWorkers,
            OutputTag<Tuple4<Integer, Long, Long, double[]>> modelOutputTag) {
        this.learningRate = learningRate;
        this.modelOutputTag = modelOutputTag;
        this.numWorkers = numWorkers;
        this.modelData = new HashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        psId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Integer, byte[]>> element) throws Exception {
        // The RPC comes here.
        // The consistency control comes here.
        // The seastar comes here.
        byte[] rpc = element.getValue().f1;
        MessageType type = MessageUtils.getMessageType(rpc, 0);
        if (type == MessageType.SPARSE_PULL_MODEL) {
            processPullRpc(rpc);
        } else {
            processOnePushRpc(rpc);
        }
    }

    private void processOnePullMessage(SparsePullModeM sparsePullModeM) {
        Preconditions.checkState(psId == sparsePullModeM.psId);
        int modelId = sparsePullModeM.modelId;
        int workerId = sparsePullModeM.workerId;
        long[] indices = sparsePullModeM.pullModelIndices.values;
        double[] pulledValues = modelData.get(modelId).getData(indices);
        // LOG.error(
        //        "[Server-{}][iteration-{}] Processing pull request from workers, with {}
        // nnzs.",
        //        psId,
        //        epochWatermark,
        //        pulledValues.length);
        PulledModelM pulledModelM =
                new PulledModelM(
                        modelId, psId, workerId, new DenseDoubleVectorStorage(pulledValues));
        output.collect(new StreamRecord<>(Tuple2.of(workerId, MessageUtils.toBytes(pulledModelM))));
    }

    private void processPullRpc(byte[] rpc) {
        Message message = MessageUtils.readFromBytes(rpc, 0);
        if (message instanceof SparsePullModeM) {
            SparsePullModeM pullModeM = (SparsePullModeM) message;
            int modelId = pullModeM.modelId;
            if (pushRequestsNumReceivedByModelId.get(modelId) == numWorkers) {
                // Processes the pending requests first.
                if (pendingPullRpcByModelId.containsKey(modelId)) {
                    List<Message> messages = pendingPullRpcByModelId.remove(modelId);
                    for (Message m : messages) {
                        processOnePullMessage((SparsePullModeM) m);
                    }
                }
                // processes this request.
                processOnePullMessage(pullModeM);
            } else {
                // Caches the pull request.
                if (pendingPullRpcByModelId.containsKey(modelId)) {
                    pendingPullRpcByModelId.get(modelId).add(pullModeM);
                } else {
                    List<Message> pullRequests = new ArrayList<>();
                    pullRequests.add(pullModeM);
                    pendingPullRpcByModelId.put(modelId, pullRequests);
                }
            }
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported pull message type: " + message.getType());
        }
    }

    private void processOnePushRpc(byte[] pushRpc) {
        Message message = MessageUtils.readFromBytes(pushRpc, 0);
        if (message instanceof PSFZeros) {
            PSFZeros psfZeros = (PSFZeros) message;
            // LOG.error(
            //        "[Server-{}][iteration-{}] Processing model initialization.",
            //        psId,
            //        epochWatermark);
            Preconditions.checkState(psId == psfZeros.psId);

            long start = psfZeros.startIndex;
            long end = psfZeros.endIndex;
            int modelId = psfZeros.modelId;
            int modelShardSize = (int) (end - start);
            modelData.put(modelId, new ServerVector(start, end, new double[modelShardSize]));
            pushRequestsNumReceivedByModelId.put(modelId, numWorkers);
        } else if (message instanceof PushGradM) {
            PushGradM pushGradM = (PushGradM) message;
            // LOG.error(
            //        "[Server-{}][iteration-{}] Processing gradient, with {} nnzs.",
            //        psId,
            //        epochWatermark,
            //        pushGradM.grad.indices.length);
            Preconditions.checkState(pushGradM.psId == psId);
            int modelId = pushGradM.modelId;

            Map<Long, Double> tmpGrad;
            double tmpWeight;
            if (sparseGradsByModelId.containsKey(modelId)) {
                tmpGrad = sparseGradsByModelId.get(modelId);
                tmpWeight = weightByModelId.get(modelId);
            } else {
                tmpGrad = new HashMap<>();
                tmpWeight = 0;
                sparseGradsByModelId.put(modelId, tmpGrad);
            }

            SparseLongDoubleVectorStorage pushedGrad = pushGradM.grad;
            tmpWeight += pushGradM.weight;
            long[] indices = pushedGrad.indices;
            double[] values = pushedGrad.values;
            for (int i = 0; i < indices.length; i++) {
                double original = tmpGrad.getOrDefault(indices[i], 0.0);
                tmpGrad.put(indices[i], original + values[i]);
            }
            weightByModelId.put(modelId, tmpWeight);

            pushRequestsNumReceivedByModelId.put(
                    modelId, pushRequestsNumReceivedByModelId.getOrDefault(modelId, 0) + 1);
            tryUpdateModel(modelId);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported message type: " + message.getClass());
        }
    }

    private void tryUpdateModel(int modelId) {
        if (pushRequestsNumReceivedByModelId.get(modelId) == numWorkers) {
            Map<Long, Double> grad = sparseGradsByModelId.get(modelId);
            TreeMap<Long, Double> sortedGrad = new TreeMap<>(grad);
            ServerVector model = modelData.get(modelId);
            for (Map.Entry<Long, Double> entry : sortedGrad.entrySet()) {
                int index = (int) ((entry.getKey()) - model.startIndex);
                model.data[index] -= entry.getValue() * learningRate / weightByModelId.get(modelId);
            }
            //
            // pushRequestsReceivedByModelId[modelId] = 0;
            // sparseGradsByModelId.remove(modelId);
            // weightByModelId.put(modelId, 0.0);
        }
    }

    @Override
    public void onEpochWatermarkIncremented(
            int epochWatermark, Context context, Collector<Tuple2<Integer, byte[]>> collector)
            throws Exception {
        // Map<Integer, Map<Long, Double>> sparseGrads = new HashMap<>();
        // double weight = 0;

        // for (byte[] rpc : pushRpcs) {
        //    Message message = MessageUtils.readFromBytes(rpc, 0);
        //    if (message instanceof PSFZeros) {
        //        PSFZeros psfZeros = (PSFZeros) message;
        //        // LOG.error(
        //        //        "[Server-{}][iteration-{}] Processing model initialization.",
        //        //        psId,
        //        //        epochWatermark);
        //        Preconditions.checkState(psId == psfZeros.psId);
        //
        //        long start = psfZeros.startIndex;
        //        long end = psfZeros.endIndex;
        //        int modelId = psfZeros.modelId;
        //        modelData.put(
        //                modelId, new ServerVector(start, end, new double[(int) (end - start)]));
        //    } else if (message instanceof PushGradM) {
        //        PushGradM pushGradM = (PushGradM) message;
        //        // LOG.error(
        //        //        "[Server-{}][iteration-{}] Processing gradient, with {} nnzs.",
        //        //        psId,
        //        //        epochWatermark,
        //        //        pushGradM.grad.indices.length);
        //        Preconditions.checkState(pushGradM.psId == psId);
        //        int modelId = pushGradM.modelId;
        //
        //        Map<Long, Double> tmpGrad;
        //        if (sparseGrads.containsKey(modelId)) {
        //            tmpGrad = sparseGrads.get(modelId);
        //        } else {
        //            tmpGrad = new HashMap<>();
        //            sparseGrads.put(modelId, tmpGrad);
        //        }
        //
        //        SparseLongDoubleVectorStorage pushedGrad = pushGradM.grad;
        //        weight += pushGradM.weight;
        //        long[] indices = pushedGrad.indices;
        //        double[] values = pushedGrad.values;
        //        for (int i = 0; i < indices.length; i++) {
        //            double original = tmpGrad.getOrDefault(indices[i], 0.0);
        //            tmpGrad.put(indices[i], original + values[i]);
        //        }
        //    } else {
        //        throw new UnsupportedOperationException(
        //                "Unsupported message type: " + message.getClass());
        //    }
        // }
        //
        // pushRpcs.clear();
        //
        //// Uses the grad to update the model.
        // for (Map.Entry<Integer, Map<Long, Double>> modelIdAndGrad : sparseGrads.entrySet()) {
        //    int modelId = modelIdAndGrad.getKey();
        //    Map<Long, Double> grad = modelIdAndGrad.getValue();
        //    TreeMap<Long, Double> sortedGrad = new TreeMap<>(grad);
        //    ServerVector model = modelData.get(modelId);
        //    for (Map.Entry<Long, Double> entry : sortedGrad.entrySet()) {
        //        int index = (int) ((entry.getKey()) - model.startIndex);
        //        model.data[index] -= entry.getValue() * learningRate / weight;
        //    }
        // }
        //
        // for (byte[] rpc : pullRpcs) {
        //    Message message = MessageUtils.readFromBytes(rpc, 0);
        //    if (message instanceof SparsePullModeM) {
        //        SparsePullModeM sparsePullModeM = (SparsePullModeM) message;
        //        Preconditions.checkState(psId == sparsePullModeM.psId);
        //        int modelId = sparsePullModeM.modelId;
        //        int workerId = sparsePullModeM.workerId;
        //        long[] indices = sparsePullModeM.pullModelIndices.values;
        //        double[] pulledValues = modelData.get(modelId).getData(indices);
        //        // LOG.error(
        //        //        "[Server-{}][iteration-{}] Processing pull request from workers, with {}
        //        // nnzs.",
        //        //        psId,
        //        //        epochWatermark,
        //        //        pulledValues.length);
        //        PulledModelM pulledModelM =
        //                new PulledModelM(
        //                        modelId,
        //                        psId,
        //                        workerId,
        //                        new DenseDoubleVectorStorage(pulledValues));
        //        collector.collect(Tuple2.of(workerId, MessageUtils.toBytes(pulledModelM)));
        //    } else {
        //        throw new UnsupportedOperationException(
        //                "Unsupported pull message type: " + message.getType());
        //    }
        // }
        // pullRpcs.clear();

        sparseGradsByModelId.clear();
        weightByModelId.clear();
        pushRequestsNumReceivedByModelId.clear();
    }

    @Override
    public void onIterationTerminated(Context context, Collector<Tuple2<Integer, byte[]>> collector)
            throws Exception {
        // outputs the model.
        for (Map.Entry<Integer, ServerVector> model : modelData.entrySet()) {
            int modelId = model.getKey();
            ServerVector serverVector = model.getValue();
            long startIndex = serverVector.startIndex;
            long endIndex = serverVector.endIndex;
            double[] data = serverVector.data;
            Tuple4<Integer, Long, Long, double[]> tuple4 =
                    Tuple4.of(modelId, startIndex, endIndex, data);
            output.collect(modelOutputTag, new StreamRecord<>(tuple4));
            LOG.error("[Server-{}]Output model at the end of iteration", psId);
        }
    }
}
