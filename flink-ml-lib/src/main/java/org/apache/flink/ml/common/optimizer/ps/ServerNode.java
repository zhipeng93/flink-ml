package org.apache.flink.ml.common.optimizer.ps;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.ml.common.optimizer.ps.message.MessageUtils;
import org.apache.flink.ml.common.optimizer.ps.message.PSFZeros;
import org.apache.flink.ml.common.optimizer.ps.message.PulledModelM;
import org.apache.flink.ml.common.optimizer.ps.message.PushGradM;
import org.apache.flink.ml.common.optimizer.ps.message.SparsePullModeM;
import org.apache.flink.ml.common.optimizer.ps.serverstorage.ServerVector;
import org.apache.flink.ml.linalg.SparseLongDoubleVector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** The server node that maintains the model parameters. */
public class ServerNode extends AbstractStreamOperator<Tuple2<Integer, byte[]>>
        implements OneInputStreamOperator<Tuple2<Integer, byte[]>, Tuple2<Integer, byte[]>>,
                IterationListener<Tuple2<Integer, byte[]>> {
    // Format of model data: model id, start index, end index, dense data.
    private final OutputTag<Tuple4<Integer, Long, Long, double[]>> modelOutputTag;
    private final int numWorkers;
    private final double alpha;
    private final double beta;
    private final double lambda1;
    private final double lambda2;

    // TODO: make it more general for all servers, not only FTRL.
    private final Map<Integer, ServerVector> modelData;
    private double[] sigma;
    private double[] z;
    private double[] n;

    private int psId = -1;

    // Only useful in sync mode.
    private final boolean sync;
    private final Map<Integer, Long2DoubleOpenHashMap> accumulatedGradByModelId = new HashMap<>();
    private final Map<Integer, Double> weightByModelId = new HashMap<>();
    private final Map<Integer, List<byte[]>> pendingPullsByModelId = new HashMap<>();

    private int numServerCores = 1;

    final SerializableObject lock = new SerializableObject();
    // for possible speed up.
    private transient ExecutorService fixedThreadPool;

    List<Future<?>> futuresInEpoch = new ArrayList<>();

    public ServerNode(
            double alpha,
            double beta,
            double lambda1,
            double lambda2,
            int numWorkers,
            OutputTag<Tuple4<Integer, Long, Long, double[]>> modelOutputTag,
            boolean sync) {
        this.alpha = alpha;
        this.beta = beta;
        this.lambda1 = lambda1;
        this.lambda2 = lambda2;
        this.modelOutputTag = modelOutputTag;
        this.numWorkers = numWorkers;
        this.modelData = new HashMap<>();
        this.sync = sync;
    }

    @Override
    public void open() throws Exception {
        super.open();
        psId = getRuntimeContext().getIndexOfThisSubtask();
        fixedThreadPool = Executors.newFixedThreadPool(numServerCores);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Integer, byte[]>> element) throws Exception {
        byte[] rpc = element.getValue().f1;
        MessageType type = MessageUtils.getMessageType(rpc, 0);
        if (type == MessageType.SPARSE_PULL_MODEL) {
            processPullRpc(rpc);
        } else {
            processOnePushRpc(rpc);
        }
    }

    private Object processPullMessage(byte[] bytesData) {
        SparsePullModeM sparsePullModeM = MessageUtils.readFromBytes(bytesData, 0);
        Preconditions.checkState(psId == sparsePullModeM.psId);
        int modelId = sparsePullModeM.modelId;
        int workerId = sparsePullModeM.workerId;
        long[] indices = sparsePullModeM.pullModelIndices;
        double[] pulledValues = modelData.get(modelId).getData(indices);
        PulledModelM pulledModelM = new PulledModelM(modelId, psId, workerId, pulledValues);
        StreamRecord<Tuple2<Integer, byte[]>> record =
                new StreamRecord<>(Tuple2.of(workerId, MessageUtils.toBytes(pulledModelM)));
        synchronized (lock) {
            output.collect(record);
        }
        return new Object();
    }

    private void processPullRpc(byte[] rpc) throws ExecutionException, InterruptedException {
        int modelId = MessageUtils.readModelIdFromSparsePullMessage(rpc, 0);
        if (sync) {
            List<byte[]> pendingPulls;
            if (pendingPullsByModelId.containsKey(modelId)) {
                pendingPulls = pendingPullsByModelId.get(modelId);
            } else {
                pendingPulls = new ArrayList<>(numWorkers);
                pendingPullsByModelId.put(modelId, pendingPulls);
            }
            pendingPulls.add(rpc);
        } else {
            futuresInEpoch.add(fixedThreadPool.submit(() -> processPullMessage(rpc)));
            // processPullMessage(rpc);
            // fixedThreadPool.execute(() -> processPullMessage(rpc));
            // StreamRecord<Tuple2<Integer, byte[]>> pulledMessage = processPullMessage(rpc);
            // Future<StreamRecord<Tuple2<Integer, byte[]>>> pulledMessage =
            // fixedThreadPool.submit(() -> processPullMessage(rpc));
            // output.collect(pulledMessage.get());
        }
    }

    private void processOnePushRpc(byte[] pushRpc) {
        MessageType type = MessageUtils.getMessageType(pushRpc, 0);
        if (type == MessageType.PSF_ZEROS) {
            PSFZeros psfZeros = MessageUtils.readFromBytes(pushRpc, 0);
            Preconditions.checkState(psId == psfZeros.psId);

            long start = psfZeros.startIndex;
            long end = psfZeros.endIndex;
            int modelId = psfZeros.modelId;
            int modelShardSize = (int) (end - start);
            if (modelData.containsKey(modelId)) {
                // Already initialized model here.
            } else {
                modelData.put(modelId, new ServerVector(start, end, new double[modelShardSize]));
                sigma = new double[modelShardSize];
                z = new double[modelShardSize];
                n = new double[modelShardSize];
            }
        } else if (type == MessageType.PUSH_GRAD) {
            // futuresInEpoch.add(fixedThreadPool.submit(() -> processPushGrad(pushRpc)));
            processPushGrad(pushRpc);
        } else {
            throw new UnsupportedOperationException("Unsupported message type: " + type);
        }
    }

    private Object processPushGrad(byte[] pushRpc) {
        PushGradM pushGradM = MessageUtils.readFromBytes(pushRpc, 0);
        Preconditions.checkState(pushGradM.psId == psId);
        int modelId = pushGradM.modelId;
        if (sync) {
            Long2DoubleOpenHashMap tmpGrad;
            double tmpWeight;
            if (accumulatedGradByModelId.containsKey(modelId)) {
                tmpGrad = accumulatedGradByModelId.get(modelId);
                tmpWeight = weightByModelId.get(modelId);
            } else {
                tmpGrad = new Long2DoubleOpenHashMap();
                tmpWeight = 0;
                accumulatedGradByModelId.put(modelId, tmpGrad);
            }

            SparseLongDoubleVector pushedGrad = pushGradM.grad;
            tmpWeight += pushGradM.weight;
            long[] indices = pushedGrad.indices;
            double[] values = pushedGrad.values;
            for (int i = 0; i < indices.length; i++) {
                double original = tmpGrad.getOrDefault(indices[i], 0.0);
                tmpGrad.put(indices[i], original + values[i]);
            }
            weightByModelId.put(modelId, tmpWeight);
        } else {
            updateModel(modelId, pushGradM.grad);
        }
        return new Object();
    }

    /** Updates the model using accumulated gradient in one iteration. */
    private void updateModel(int modelId, Long2DoubleOpenHashMap accumulatedGrad) {
        ServerVector model = modelData.get(modelId);
        for (Map.Entry<Long, Double> entry : accumulatedGrad.entrySet()) {
            int index = (int) ((entry.getKey()) - model.startIndex);
            double gi = entry.getValue();
            updateModelOnOneDim(gi, index, model);
        }
    }

    /** Updates model using one received gradient. */
    private void updateModel(int modelId, SparseLongDoubleVector grad) {
        ServerVector model = modelData.get(modelId);
        for (int i = 0; i < grad.indices.length; i++) {
            int index = (int) (grad.indices[i] - model.startIndex);
            double gi = grad.values[i];
            updateModelOnOneDim(gi, index, model);
        }
    }

    private void updateModelOnOneDim(double gi, int index, ServerVector model) {
        double gigi = gi * gi;
        sigma[index] = 1 / alpha * (Math.sqrt(n[index] + gigi) - Math.sqrt(n[index]));
        z[index] += gi - sigma[index] * model.data[index];
        n[index] += gigi;

        if (Math.abs(z[index]) <= lambda1) {
            model.data[index] = 0;
        } else {
            model.data[index] =
                    -(z[index] - Math.signum(z[index]) * lambda1)
                            / ((beta + Math.sqrt(n[index])) / alpha + lambda2);
        }
    }

    @Override
    public void onEpochWatermarkIncremented(
            int epochWatermark, Context context, Collector<Tuple2<Integer, byte[]>> collector)
            throws InterruptedException, ExecutionException {
        if (sync) {
            // Updates model.
            int modelId = 0;
            Long2DoubleOpenHashMap grad = accumulatedGradByModelId.remove(modelId);
            if (grad != null) {
                // The first iteration contains no pulls.
                Preconditions.checkState(epochWatermark != 0);
                updateModel(modelId, grad);
            }
            List<byte[]> pendingPulls = pendingPullsByModelId.remove(modelId);
            if (pendingPulls != null) {
                // The last iteration contains no pulls.
                for (byte[] pull : pendingPulls) {
                    // fixedThreadPool.submit(() -> processPullMessage(pull));
                    processPullMessage(pull);
                }
            }
        } else {
            for (Future<?> future : futuresInEpoch) {
                future.get();
            }
            futuresInEpoch.clear();
        }
    }

    @Override
    public void onIterationTerminated(
            Context context, Collector<Tuple2<Integer, byte[]>> collector) {
        for (Map.Entry<Integer, ServerVector> model : modelData.entrySet()) {
            int modelId = model.getKey();
            ServerVector serverVector = model.getValue();
            long startIndex = serverVector.startIndex;
            long endIndex = serverVector.endIndex;
            double[] data = serverVector.data;
            Tuple4<Integer, Long, Long, double[]> tuple4 =
                    Tuple4.of(modelId, startIndex, endIndex, data);
            output.collect(modelOutputTag, new StreamRecord<>(tuple4));
        }
    }

    private static class SerializableObject implements Serializable {}
}
