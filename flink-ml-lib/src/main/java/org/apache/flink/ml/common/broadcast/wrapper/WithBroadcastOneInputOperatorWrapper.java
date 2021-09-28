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
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper for {@link OneInputStreamOperator} that calls ${@link
 * org.apache.flink.ml.common.broadcast.BroadcastUtils}.
 *
 * @param <IN>
 * @param <OUT>
 */
public class WithBroadcastOneInputOperatorWrapper<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {
    /** wrapped op. */
    OneInputStreamOperator<IN, OUT> wrappedOp;
    /** mailbox executor. */
    MailboxExecutor mailboxExecutor;
    /** name of broadcast DataStreams. */
    final String[] broadcastStreamNames;
    /** memory used to store cached elements. */
    List<IN> cache;
    /** whether this input should be blocking. */
    private final boolean isBlocking;
    /**
     * the initial value of broadcastVariablesReady should be isBlocking[i], after all broadcast
     * variables are initialized, it is always false.
     */
    boolean broadcastVariablesReady;
    /** state. */
    ListState<IN> opState;
    /** input type. */
    TypeInformation<IN> inType;

    public WithBroadcastOneInputOperatorWrapper(
            OneInputStreamOperator<IN, OUT> op,
            TypeInformation<IN> inType,
            String[] broadcastStreamNames,
            boolean isBlocking) {

        this.wrappedOp = op;
        this.inType = inType;
        this.broadcastStreamNames = broadcastStreamNames;
        this.isBlocking = isBlocking;

        if (isBlocking) {
            this.cache = new ArrayList<>();
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
    public void processElement(StreamRecord<IN> streamRecord) throws Exception {
        if (isBlocking) {
            // this non-broadcast input needs to be cached.
            if (areBroadcastVariablesReady()) {
                // finish all the cached elements and start to process the next elements
                for (IN ele : cache) {
                    wrappedOp.processElement(new StreamRecord<>(ele));
                }
                cache.clear();
                wrappedOp.processElement(streamRecord);

            } else {
                // cache the current elements
                cache.add(streamRecord.getValue());
            }

        } else {
            if (areBroadcastVariablesReady()) {
                wrappedOp.processElement(streamRecord);
            } else {
                while (!areBroadcastVariablesReady()) {
                    mailboxExecutor.yield();
                }
                wrappedOp.processElement(streamRecord);
            }
        }
    }

    @Override
    public void endInput() throws Exception {
        while (!areBroadcastVariablesReady()) {
            mailboxExecutor.yield();
        }
        for (IN ele : cache) {
            wrappedOp.processElement(new StreamRecord<>(ele));
        }
        if (wrappedOp instanceof BoundedOneInput) {
            ((BoundedOneInput) wrappedOp).endInput();
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
        opState.clear();
        opState.addAll(cache);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        opState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<IN>("cached data", this.inType));
    }
}
