package org.apache.flink.ml.common.broadcast;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.io.Serializable;

/**
 * Factory class for {@link CacheBroadcastVariablesStreamOperator}.
 *
 * @param <OUT>
 */
public class CacheBroadcastVariablesStreamOperatorFactory<OUT>
        extends AbstractStreamOperatorFactory<OUT> implements Serializable {
    /** name of the broadcast streams that this operator holds. */
    String[] bcNames;
    /** Types of the input DataStreams. */
    TypeInformation<?>[] inTypes;

    public CacheBroadcastVariablesStreamOperatorFactory(
            String[] bcNames, TypeInformation<?>[] inTypes) {
        this.bcNames = bcNames;
        this.inTypes = inTypes;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        return (T) new CacheBroadcastVariablesStreamOperator(parameters, bcNames, inTypes);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return CacheBroadcastVariablesStreamOperator.class;
    }
}
