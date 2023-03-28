/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.common.optimizer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.feature.LabeledLargePointWithWeight;
import org.apache.flink.ml.common.lossfunc.LossFunc;
import org.apache.flink.ml.common.optimizer.ps.MirrorWorkerNode;
import org.apache.flink.ml.common.optimizer.ps.ServerNode;
import org.apache.flink.ml.common.optimizer.ps.WorkerNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Stochastic Gradient Descent (SGD) is the mostly wide-used optimizer for optimizing machine
 * learning models. It iteratively makes small adjustments to the machine learning model according
 * to the gradient at each step, to decrease the error of the model.
 *
 * <p>See https://en.wikipedia.org/wiki/Stochastic_gradient_descent.
 */
@Internal
public class PSFtrl {
    static final Logger LOG = LoggerFactory.getLogger(PSFtrl.class);

    /** Params for SGD optimizer. */
    private final FTRLParams params;

    private final int numPs;

    public PSFtrl(
            int numPs,
            int maxIter,
            double alpha,
            double beta,
            int globalBatchSize,
            long modelDim,
            double tol,
            double reg,
            double elasticNet,
            boolean sync) {
        this.numPs = numPs;
        this.params =
                new FTRLParams(
                        maxIter,
                        alpha,
                        beta,
                        globalBatchSize,
                        modelDim,
                        tol,
                        reg,
                        elasticNet,
                        sync);
    }

    public DataStream<Tuple4<Integer, Long, Long, double[]>> optimize(
            DataStream<LabeledLargePointWithWeight> trainData, LossFunc lossFunc) {

        // Initialize the model for each ps piece.
        StreamExecutionEnvironment env = trainData.getExecutionEnvironment();
        DataStream<byte[]> variableStream = env.fromElements(new byte[0]).broadcast().map(x -> x);
        // .filter(x -> x.length > 0);
        // modelDim.broadcast()
        //        .map(
        //                new MapFunction<Long, byte[]>() {
        //                    @Override
        //                    public byte[] map(Long value) throws Exception {
        //                        byte[] buffer = new byte[Long.BYTES];
        //                        Bits.putLong(buffer, 0, value);
        //                        return buffer;
        //                    }
        //                });

        DataStreamList resultList =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(variableStream),
                        ReplayableDataStreamList.notReplay(trainData.rebalance().map(x -> x)),
                        IterationConfig.newBuilder().build(),
                        new TrainIterationBody(lossFunc, params, numPs));

        return resultList.get(0);
    }

    /** The iteration implementation for training process. */
    private static class TrainIterationBody implements IterationBody {
        private final LossFunc lossFunc;
        private final FTRLParams params;

        private final int numPss;

        public TrainIterationBody(LossFunc lossFunc, FTRLParams params, int numPss) {
            this.lossFunc = lossFunc;
            this.params = params;
            this.numPss = numPss;
        }

        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<byte[]> variableStream = variableStreams.get(0);
            DataStream<LabeledLargePointWithWeight> trainData = dataStreams.get(0);
            final OutputTag<Tuple4<Integer, Long, Long, double[]>> modelDataOutputTag =
                    new OutputTag<Tuple4<Integer, Long, Long, double[]>>("MODEL_OUTPUT") {};

            // psId, Messages (message could be push or pull.
            SingleOutputStreamOperator<Tuple2<Integer, byte[]>> messageToPS =
                    trainData
                            .connect(variableStream)
                            .transform(
                                    "workerNode",
                                    new TupleTypeInfo(
                                            Types.INT,
                                            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
                                    new WorkerNode(lossFunc, params, numPss));
            int numWorkers = messageToPS.getParallelism();

            //
            SingleOutputStreamOperator<Tuple2<Integer, byte[]>> messageToWorker =
                    messageToPS
                            .partitionCustom(
                                    new Partitioner<Integer>() {
                                        @Override
                                        public int partition(Integer key, int numPartitions) {
                                            return key % numPartitions;
                                        }
                                    },
                                    new KeySelector<Tuple2<Integer, byte[]>, Integer>() {
                                        @Override
                                        public Integer getKey(Tuple2<Integer, byte[]> value)
                                                throws Exception {
                                            return value.f0;
                                        }
                                    })
                            .transform(
                                    "ServerNode",
                                    new TupleTypeInfo(
                                            Types.INT,
                                            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
                                    new ServerNode(
                                            params.alpha,
                                            params.beta,
                                            params.reg,
                                            params.elasticNet,
                                            numWorkers,
                                            modelDataOutputTag,
                                            params.sync));
            messageToWorker.setParallelism(numPss);
            // messageToWorker.getTransformation().setSlotSharingGroup("ServerNode");

            DataStream<byte[]> combinedMessageToWorker =
                    messageToWorker
                            .partitionCustom(
                                    new Partitioner<Integer>() {
                                        @Override
                                        public int partition(Integer key, int numPartitions) {
                                            return key % numPartitions;
                                        }
                                    },
                                    new KeySelector<Tuple2<Integer, byte[]>, Integer>() {
                                        @Override
                                        public Integer getKey(Tuple2<Integer, byte[]> value)
                                                throws Exception {
                                            return value.f0;
                                        }
                                    })
                            .transform(
                                    "MirrorWorkerNode",
                                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                                    new MirrorWorkerNode(numPss))
                            .setParallelism(numWorkers);

            return new IterationBodyResult(
                    DataStreamList.of(combinedMessageToWorker),
                    DataStreamList.of(messageToWorker.getSideOutput(modelDataOutputTag)),
                    null);
        }
    }

    /** FTRL params. */
    public static class FTRLParams implements Serializable {
        public final int maxIter;
        public final double alpha;
        public final double beta;
        public final int globalBatchSize;

        public final long modelDim;
        public final double tol;
        public final double reg;
        public final double elasticNet;

        public final boolean sync;

        private FTRLParams(
                int maxIter,
                double alpha,
                double beta,
                int globalBatchSize,
                long modelDim,
                double tol,
                double reg,
                double elasticNet,
                boolean sync) {
            this.maxIter = maxIter;
            this.alpha = alpha;
            this.beta = beta;
            this.globalBatchSize = globalBatchSize;
            this.modelDim = modelDim;
            this.tol = tol;
            this.reg = reg;
            this.elasticNet = elasticNet;
            this.sync = sync;
        }
    }
}
