package org.apache.flink.ml.common.broadcast.wrapper;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.broadcast.BroadcastContext;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactoryUtil;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper for {@link MultipleInputStreamOperator} that calls ${@link
 * org.apache.flink.ml.common.broadcast.BroadcastUtils}.
 *
 * @param <OUT>
 */
public class WithBroadcastMutliInputOperatorWrapper<OUT> extends AbstractStreamOperatorV2<OUT>
        implements MultipleInputStreamOperator<OUT>, BoundedMultiInput {
    /** wrapped op. */
    MultipleInputStreamOperator wrappedOp;
    /** mailbox executor. */
    MailboxExecutor mailboxExecutor;
    /** input list. */
    List<Input> inputList;
    /** whether each input should block. */
    boolean[] isBlocking;
    /** input types. */
    TypeInformation<?>[] inTypes;
    /** name of broadcast DataStreams. */
    String[] broadcastStreamNames;
    /**
     * the initial value of broadcastVariablesReady should be isBlocking[i], after all broadcast
     * variables are initialized, it is always false.
     */
    boolean broadcastVariablesReady;

    public WithBroadcastMutliInputOperatorWrapper(
            StreamOperatorParameters<OUT> parameters,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<?>[] inTypes,
            String[] broadcastStreamNames,
            boolean[] isBlocking) {
        super(parameters, inTypes.length);
        this.wrappedOp =
                (MultipleInputStreamOperator)
                        StreamOperatorFactoryUtil.createOperator(
                                        operatorFactory,
                                        (StreamTask) parameters.getContainingTask(),
                                        parameters.getStreamConfig(),
                                        parameters.getOutput(),
                                        parameters.getOperatorEventDispatcher())
                                .f0;
        this.inTypes = inTypes;
        this.broadcastStreamNames = broadcastStreamNames;
        this.isBlocking = isBlocking;
        mailboxExecutor =
                parameters
                        .getContainingTask()
                        .getMailboxExecutorFactory()
                        .createExecutor(TaskMailbox.MIN_PRIORITY);

        // construct the wrapped input list
        int numInputs = wrappedOp.getInputs().size();
        this.inputList = new ArrayList<>(numInputs);
        for (int i = 0; i < numInputs; i++) {
            // wrap the inputs
            inputList.add(new ProxyInput(i + 1));
        }
    }

    /**
     * check whether one input should be blocking.
     *
     * @param inputId the input id.
     * @return true if it is blocking, false otherwise.
     */
    public boolean isBlocking(int inputId) {
        return this.isBlocking[inputId - 1];
    }

    @Override
    public void endInput(int inputId) throws Exception {
        // deal with the cached elements
        ((ProxyInput) (this.getInputs().get(inputId - 1))).endInput();

        Input originInput = (Input) wrappedOp.getInputs().get(inputId - 1);
        if (originInput instanceof BoundedOneInput) {
            ((BoundedOneInput) originInput).endInput();
        }
    }

    @Override
    public List<Input> getInputs() {
        return this.inputList;
    }

    private <IN> void processElement(StreamRecord var1, Input<IN> input) throws Exception {
        input.processElement(var1);
    }

    private <IN> void processWatermark(Watermark watermark, Input<IN> input) throws Exception {
        input.processWatermark(watermark);
    }

    private <IN> void processLatencyMarker(LatencyMarker latencyMarker, Input<IN> input)
            throws Exception {
        input.processLatencyMarker(latencyMarker);
    }

    private <IN> void setKeyContextElement(StreamRecord streamRecord, Input<IN> input)
            throws Exception {
        input.setKeyContextElement(streamRecord);
    }

    private <IN> void processWatermarkStatus(WatermarkStatus watermarkStatus, Input<IN> input)
            throws Exception {
        input.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void open() throws Exception {
        super.open();
        wrappedOp.open();
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        wrappedOp.finish();
    }

    @Override
    public void close() throws Exception {
        super.close();
        wrappedOp.close();
        for (int i = 0; i < broadcastStreamNames.length; i++) {
            BroadcastContext.tryClearBroadcastVariable(broadcastStreamNames[i]);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        wrappedOp.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        wrappedOp.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        for (int i = 0; i < inputList.size(); i++) {
            ((ProxyInput) inputList.get(i)).snapshotState(context);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        for (int i = 0; i < inputList.size(); i++) {
            ((ProxyInput) inputList.get(i)).initializeState(context);
        }
    }

    private boolean areBroadcastVariablesReady() {
        if (broadcastVariablesReady) {
            return true;
        }
        for (int i = 0; i < broadcastStreamNames.length; i++) {
            if (!BroadcastContext.isBroadcastVariableReady(broadcastStreamNames[i])) {
                return false;
            }
        }
        broadcastVariablesReady = true;
        return true;
    }

    /**
     * wrapper for Input that used in {@link WithBroadcastMutliInputOperatorWrapper}.
     *
     * @param <IN>
     */
    class ProxyInput<IN> implements Input<IN> {
        /** cache elements in this input. */
        List<IN> cache;
        /** state. */
        ListState<IN> state;
        /** input id. */
        int inputId;
        /** input. */
        Input<IN> input;

        public ProxyInput(int inputId) {
            this.inputId = inputId;
            this.cache = new ArrayList<>();
            this.input = (Input<IN>) wrappedOp.getInputs().get(inputId - 1);
        }

        @Override
        public void processElement(StreamRecord<IN> element) throws Exception {
            if (isBlocking(inputId)) {
                // this non-broadcast input needs to be cached.
                if (areBroadcastVariablesReady()) {
                    // finish all the cached elements and start to process the next elements
                    for (IN ele : cache) {
                        WithBroadcastMutliInputOperatorWrapper.this.processElement(
                                new StreamRecord<>(ele), input);
                    }
                    cache.clear();
                    WithBroadcastMutliInputOperatorWrapper.this.processElement(element, input);

                } else {
                    // cache the current elements
                    cache.add(element.getValue());
                }

            } else {
                if (areBroadcastVariablesReady()) {
                    WithBroadcastMutliInputOperatorWrapper.this.processElement(element, input);
                } else {
                    while (!areBroadcastVariablesReady()) {
                        mailboxExecutor.yield();
                    }
                    WithBroadcastMutliInputOperatorWrapper.this.processElement(element, input);
                }
            }
        }

        @Override
        public void processWatermark(Watermark var1) throws Exception {
            WithBroadcastMutliInputOperatorWrapper.this.processWatermark(var1, input);
        }

        @Override
        public void processWatermarkStatus(WatermarkStatus var1) throws Exception {
            WithBroadcastMutliInputOperatorWrapper.this.processWatermarkStatus(var1, input);
        }

        @Override
        public void processLatencyMarker(LatencyMarker var1) throws Exception {
            WithBroadcastMutliInputOperatorWrapper.this.processLatencyMarker(var1, input);
        }

        @Override
        public void setKeyContextElement(StreamRecord var1) throws Exception {
            WithBroadcastMutliInputOperatorWrapper.this.setKeyContextElement(var1, input);
        }

        public void endInput() throws Exception {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            for (IN ele : cache) {
                WithBroadcastMutliInputOperatorWrapper.this.processElement(
                        new StreamRecord<>(ele), input);
            }
        }

        private void snapshotState(StateSnapshotContext context) throws Exception {
            state.clear();
            state.addAll(cache);
        }

        private void initializeState(StateInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<IN>(
                                            "cached-inputId_" + inputId,
                                            (TypeInformation<IN>) inTypes[inputId - 1]));
        }
    }
}
