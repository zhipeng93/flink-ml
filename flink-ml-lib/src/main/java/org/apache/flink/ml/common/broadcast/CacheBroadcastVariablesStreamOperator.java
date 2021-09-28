package org.apache.flink.ml.common.broadcast;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The operator that process all broadcast inputs and stores them in static {@link
 * BroadcastContext}.
 */
public class CacheBroadcastVariablesStreamOperator<OUT> extends AbstractStreamOperatorV2<OUT>
        implements MultipleInputStreamOperator<OUT>, BoundedMultiInput, Serializable {
    /** names of the broadcast streams. */
    final String[] broadcastNames;
    /** input list of the multi-input operator. */
    final List<Input> inputList;
    /** type information of the inputs. */
    final TypeInformation<?>[] inTypes;

    public CacheBroadcastVariablesStreamOperator(
            StreamOperatorParameters<OUT> parameters,
            String[] broadcastNames,
            TypeInformation<?>[] inTypes) {
        super(parameters, broadcastNames.length);
        this.broadcastNames = broadcastNames;
        this.inTypes = inTypes;

        inputList = new ArrayList<>();
        for (int i = 0; i < broadcastNames.length; i++) {
            inputList.add(new ProxyInput(this, i + 1));
        }
    }

    @Override
    public List<Input> getInputs() {
        return inputList;
    }

    @Override
    public void endInput(int i) throws Exception {
        ((ProxyInput) inputList.get(i - 1)).endInput();
    }

    /**
     * write cached broadcast inputs & blocking input that has not been consumed.
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        for (int inputId = 0; inputId < inputList.size(); inputId++) {
            ((ProxyInput) getInputs().get(inputId)).snapshotState(context);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        for (int inputId = 0; inputId < inputList.size(); inputId++) {
            ((ProxyInput) getInputs().get(inputId)).initializeState(context);
        }
    }

    /**
     * Utility class to read and cache broadcast inputs.
     *
     * @param <IN>
     */
    class ProxyInput<IN, OUT> extends AbstractInput<IN, OUT> {
        /** used to cache the broadcast input. */
        List<IN> cache;
        /** whether this subtask needs to store the input list. */
        boolean needsMaterialize;
        /** state. */
        ListState<IN> opState;

        public ProxyInput(AbstractStreamOperatorV2<OUT> owner, int inputId) {
            super(owner, inputId);
            int partitionId = owner.getRuntimeContext().getIndexOfThisSubtask();
            this.cache = new ArrayList<>();
            needsMaterialize = BroadcastContext.tryPutCacheList(broadcastNames[inputId - 1], cache);
            if (needsMaterialize) {
                System.out.println(
                        "cz----this subtask needs to cache the broadcast input. partitionId: "
                                + partitionId
                                + ", variableName: "
                                + broadcastNames[inputId - 1]);
            }
        }

        @Override
        public void processElement(StreamRecord<IN> element) throws Exception {
            if (needsMaterialize) {
                cache.add(element.getValue());
            }
        }

        public void endInput() {
            if (needsMaterialize) {
                BroadcastContext.setBroadcastVariableReady(broadcastNames[inputId - 1]);
            }
        }

        public void snapshotState(StateSnapshotContext context) throws Exception {
            opState.addAll(cache);
        }

        public void initializeState(StateInitializationContext context) throws Exception {
            opState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<IN>(
                                            "cache1", (TypeInformation<IN>) inTypes[inputId - 1]));
        }
    }
}
