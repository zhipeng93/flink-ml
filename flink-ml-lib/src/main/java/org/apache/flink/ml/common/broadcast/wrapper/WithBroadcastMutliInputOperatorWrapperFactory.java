package org.apache.flink.ml.common.broadcast.wrapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.io.Serializable;

/**
 * Factory class for {@link WithBroadcastMutliInputOperatorWrapper}.
 *
 * @param <OUT>
 */
public class WithBroadcastMutliInputOperatorWrapperFactory<OUT>
        extends AbstractStreamOperatorFactory<OUT> implements Serializable {
    /** operatorFactory for MultiInputStreamOperator. */
    StreamOperatorFactory operatorFactory;
    /** input types. */
    TypeInformation<?>[] inTypes;
    /** name of broadcast DataStreams. */
    String[] broadcastInputNames;
    /** whether each input should block. */
    boolean[] isBlocking;

    public WithBroadcastMutliInputOperatorWrapperFactory(
            StreamOperatorFactory operatorFactory,
            TypeInformation<?>[] inTypes,
            String[] broadcastInputNames,
            boolean[] isBlocking) {
        this.operatorFactory = operatorFactory;
        this.inTypes = inTypes;
        this.broadcastInputNames = broadcastInputNames;
        this.isBlocking = isBlocking;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        return (T)
                new WithBroadcastMutliInputOperatorWrapper<OUT>(
                        parameters, operatorFactory, inTypes, broadcastInputNames, isBlocking);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return WithBroadcastMutliInputOperatorWrapper.class;
    }
}
