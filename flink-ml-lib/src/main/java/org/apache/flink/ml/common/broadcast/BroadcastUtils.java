package org.apache.flink.ml.common.broadcast;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.broadcast.wrapper.WithBroadcastMutliInputOperatorWrapperFactory;
import org.apache.flink.ml.common.broadcast.wrapper.WithBroadcastOneInputOperatorWrapper;
import org.apache.flink.ml.common.broadcast.wrapper.WithBroadcastTwoInputOperatorWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

/** A utility class for withBroadcast(). */
public class BroadcastUtils {

    private static <OUT> DataStream<OUT> cacheBroadcastVariables(
            StreamExecutionEnvironment env,
            Map<String, DataStream<?>> bcStreams,
            TypeInformation<OUT> outType) {
        int numBroadcastInput = bcStreams.size();
        String[] broadcastInputNames = bcStreams.keySet().toArray(new String[0]);
        DataStream<?>[] broadcastInputs = bcStreams.values().toArray(new DataStream<?>[0]);
        TypeInformation<?>[] broadcastInTypes = new TypeInformation[numBroadcastInput];
        for (int i = 0; i < numBroadcastInput; i++) {
            broadcastInTypes[i] = broadcastInputs[i].getType();
        }

        MultipleInputTransformation<OUT> transformation =
                new MultipleInputTransformation<OUT>(
                        "broadcastInputs",
                        new CacheBroadcastVariablesStreamOperatorFactory<OUT>(
                                broadcastInputNames, broadcastInTypes),
                        outType,
                        env.getParallelism());
        for (DataStream<?> dataStream : bcStreams.values()) {
            transformation.addInput(dataStream.broadcast().getTransformation());
        }
        env.addOperator(transformation);
        return new MultipleConnectedStreams(env).transform(transformation);
    }

    private static String getCoLocationKey(String[] broadcastNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("FLINKML-broadcast-colocation");
        for (String name : broadcastNames) {
            sb.append(name);
        }
        return sb.toString();
    }

    public static <IN, OUT> DataStream<OUT> withBroadcastSet(
            DataStream<IN> input,
            OneInputStreamOperator<IN, OUT> op,
            TypeInformation<OUT> outType,
            Map<String, DataStream<?>> bcStreams,
            boolean isBlocking) {
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        String[] broadcastInputNames = bcStreams.keySet().toArray(new String[0]);
        final String coLocationKey = getCoLocationKey(broadcastInputNames);

        DataStream<OUT> cachedBroadcastStream = cacheBroadcastVariables(env, bcStreams, outType);
        cachedBroadcastStream.getTransformation().setCoLocationGroupKey(coLocationKey);
        input.getTransformation().setCoLocationGroupKey(coLocationKey);

        DataStream<OUT> finalOut =
                input.transform(
                        "func",
                        outType,
                        new WithBroadcastOneInputOperatorWrapper<IN, OUT>(
                                op, input.getType(), broadcastInputNames, isBlocking));

        return cachedBroadcastStream.union(finalOut);
    }

    public static <IN1, IN2, OUT> DataStream<OUT> withBroadcastSet(
            DataStream<IN1> input1,
            DataStream<IN2> input2,
            TwoInputStreamOperator<IN1, IN2, OUT> op,
            TypeInformation<OUT> outType,
            Map<String, DataStream<?>> bcStreams,
            boolean[] isBlocking) {
        StreamExecutionEnvironment env = input1.getExecutionEnvironment();
        String[] broadcastInputNames = bcStreams.keySet().toArray(new String[0]);
        final String coLocationKey = getCoLocationKey(broadcastInputNames);

        DataStream<OUT> cachedBroadcastStream = cacheBroadcastVariables(env, bcStreams, outType);
        cachedBroadcastStream.getTransformation().setCoLocationGroupKey(coLocationKey);

        input1.getTransformation().setCoLocationGroupKey(coLocationKey);
        input2.getTransformation().setCoLocationGroupKey(coLocationKey);

        DataStream<OUT> finalOut =
                input1.connect(input2)
                        .transform(
                                "func",
                                outType,
                                new WithBroadcastTwoInputOperatorWrapper<>(
                                        op,
                                        input1.getType(),
                                        input2.getType(),
                                        broadcastInputNames,
                                        isBlocking));

        return cachedBroadcastStream.union(finalOut);
    }

    public static <OUT> DataStream<OUT> withBroadcastSet(
            List<DataStream<?>> inputList,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<OUT> outType,
            Map<String, DataStream<?>> bcStreams,
            boolean[] isBlocking) {
        Preconditions.checkState(inputList.size() >= 1);
        StreamExecutionEnvironment env = inputList.get(0).getExecutionEnvironment();
        String[] broadcastInputNames = bcStreams.keySet().toArray(new String[0]);
        final String coLocationKey = getCoLocationKey(broadcastInputNames);

        DataStream<OUT> cachedBroadcastStream = cacheBroadcastVariables(env, bcStreams, outType);
        cachedBroadcastStream.getTransformation().setCoLocationGroupKey(coLocationKey);

        TypeInformation<?>[] nonBroadcastInTypes = new TypeInformation[inputList.size()];
        for (int i = 0; i < nonBroadcastInTypes.length; i++) {
            nonBroadcastInTypes[i] = inputList.get(i).getType();
            inputList.get(i).getTransformation().setCoLocationGroupKey(coLocationKey);
        }

        MultipleInputTransformation<OUT> nonBroadcastTransformation =
                new MultipleInputTransformation<OUT>(
                        "non-broadcastInputs",
                        new WithBroadcastMutliInputOperatorWrapperFactory<>(
                                operatorFactory,
                                nonBroadcastInTypes,
                                broadcastInputNames,
                                isBlocking),
                        outType,
                        env.getParallelism());
        for (DataStream<?> dataStream : inputList) {
            nonBroadcastTransformation.addInput(dataStream.getTransformation());
        }

        DataStream<OUT> finalOut =
                new MultipleConnectedStreams(env).transform(nonBroadcastTransformation);
        return cachedBroadcastStream.union(finalOut);
    }
}
