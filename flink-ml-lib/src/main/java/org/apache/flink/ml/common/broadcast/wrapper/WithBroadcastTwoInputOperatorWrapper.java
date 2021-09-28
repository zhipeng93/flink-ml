package org.apache.flink.ml.common.broadcast.wrapper;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.broadcast.BroadcastContext;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper for {@link TwoInputStreamOperator} that calls ${@link
 * org.apache.flink.ml.common.broadcast.BroadcastUtils}.
 *
 * @param <IN1>
 * @param <IN2>
 * @param <OUT>
 */
public class WithBroadcastTwoInputOperatorWrapper<IN1, IN2, OUT> extends AbstractStreamOperator<OUT>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, BoundedMultiInput {
    /** wrapped op. */
    TwoInputStreamOperator<IN1, IN2, OUT> wrappedOp;
    /** mailbox executor. */
    MailboxExecutor mailboxExecutor;
    /** name of broadcast DataStreams. */
    final String[] broadcastStreamNames;
    /** whether each input should block. */
    boolean[] isBlocking;
    /** cache1. */
    List<IN1> cache1;
    /** cache2. */
    List<IN2> cache2;
    /**
     * the initial value of broadcastVariablesReady should be isBlocking[i], after all broadcast
     * variables are initialized, it is always false.
     */
    boolean broadcastVariablesReady;
    /** state1. */
    ListState<IN1> opState1;
    /** state2. */
    ListState<IN2> opState2;
    /** type of input1. */
    TypeInformation<IN1> inType1;
    /** type of input2. */
    TypeInformation<IN2> inType2;

    public WithBroadcastTwoInputOperatorWrapper(
            TwoInputStreamOperator<IN1, IN2, OUT> wrappedOp,
            TypeInformation<IN1> inType1,
            TypeInformation<IN2> inType2,
            String[] broadcastStreamNames,
            boolean[] isblocking) {
        this.wrappedOp = wrappedOp;
        this.inType1 = inType1;
        this.inType2 = inType2;
        this.broadcastStreamNames = broadcastStreamNames;
        this.isBlocking = isblocking;

        if (isblocking[0]) {
            cache1 = new ArrayList<>();
        }
        if (isblocking[1]) {
            cache2 = new ArrayList<>();
        }
        broadcastVariablesReady = false;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        ((AbstractStreamOperator<OUT>) wrappedOp).setup(containingTask, config, output);
        mailboxExecutor =
                getContainingTask()
                        .getMailboxExecutorFactory()
                        .createExecutor(TaskMailbox.MIN_PRIORITY);
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

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        if (isBlocking[0]) {
            // this non-broadcast input needs to be cached.
            if (areBroadcastVariablesReady()) {
                // finish all the cached elements and start to process the next elements
                for (IN1 ele : cache1) {
                    wrappedOp.processElement1(new StreamRecord<>(ele));
                }
                cache1.clear();
                wrappedOp.processElement1(element);

            } else {
                // cache the current elements
                cache1.add(element.getValue());
            }

        } else {
            if (areBroadcastVariablesReady()) {
                wrappedOp.processElement1(element);
            } else {
                while (!areBroadcastVariablesReady()) {
                    mailboxExecutor.yield();
                }
                wrappedOp.processElement1(element);
            }
        }
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        if (isBlocking[1]) {
            // this non-broadcast input needs to be cached.
            if (areBroadcastVariablesReady()) {
                // finish all the cached elements and start to process the next elements
                for (IN2 ele : cache2) {
                    wrappedOp.processElement2(new StreamRecord<>(ele));
                }
                cache2.clear();
                wrappedOp.processElement2(element);

            } else {
                // cache the current elements
                cache2.add(element.getValue());
            }

        } else {
            if (areBroadcastVariablesReady()) {
                wrappedOp.processElement2(element);
            } else {
                while (!areBroadcastVariablesReady()) {
                    mailboxExecutor.yield();
                }
                wrappedOp.processElement2(element);
            }
        }
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (inputId == 1) {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            for (IN1 ele : cache1) {
                wrappedOp.processElement1(new StreamRecord<>(ele));
            }
            if (wrappedOp instanceof BoundedMultiInput) {
                ((BoundedMultiInput) wrappedOp).endInput(1);
            }
        } else if (inputId == 2) {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            for (IN2 ele : cache2) {
                wrappedOp.processElement2(new StreamRecord<>(ele));
            }
            if (wrappedOp instanceof BoundedMultiInput) {
                ((BoundedMultiInput) wrappedOp).endInput(2);
            }
        }
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
        opState1.clear();
        opState1.addAll(cache1);
        opState2.clear();
        opState2.addAll(cache2);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        opState1 =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<IN1>("cache1", inType1));
        opState2 =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<IN2>("cache2", inType2));
    }
}
