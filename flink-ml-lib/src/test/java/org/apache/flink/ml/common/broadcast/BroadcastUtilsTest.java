package org.apache.flink.ml.common.broadcast;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.NumberSequenceIterator;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** Test for {@link BroadcastUtils}. */
public class BroadcastUtilsTest {

    static long factor = 1000000;

    private StreamExecutionEnvironment getEnv(boolean enableCheckpoints) {
        Configuration conf = new Configuration();
        if (enableCheckpoints) {
            ConfigOption<Boolean> enableCheckpointAfterTaskFinish =
                    ConfigOptions.key(
                                    "execution.checkpointing.checkpoints-after-tasks-finish.enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "Feature toggle for enabling checkpointing after tasks finish.");
            conf.set(enableCheckpointAfterTaskFinish, true);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        if (enableCheckpoints) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
            env.getCheckpointConfig().setCheckpointStorage("file:///tmp/rocksDB");
            env.enableCheckpointing(10000);
        }
        return env;
    }

    private long count(DataStream<Long> input) throws Exception {
        List<Long> cnts =
                IteratorUtils.toList(
                        input.transform(
                                        "count",
                                        BasicTypeInfo.LONG_TYPE_INFO,
                                        new CountOneInputOperator())
                                .executeAndCollect());
        long sum = 0;
        for (long s : cnts) {
            sum += s;
        }
        return sum;
    }

    static class CountOneInputOperator extends AbstractStreamOperator<Long>
            implements OneInputStreamOperator<Long, Long>, BoundedOneInput {
        long cnt = 0;

        @Override
        public void processElement(StreamRecord streamRecord) throws Exception {
            cnt++;
        }

        @Override
        public void endInput() throws Exception {
            output.collect(new StreamRecord<>(cnt));
        }
    }

    /** Test class for testOneInput(). */
    static class OneInputSumOp extends AbstractStreamOperator<Long>
            implements OneInputStreamOperator<Long, Long> {
        @Override
        public void processElement(StreamRecord<Long> streamRecord) throws Exception {
            List<Long> source1 = BroadcastContext.getBroadcastVariable("source1");
            List<Long> source2 = BroadcastContext.getBroadcastVariable("source2");
            assert source1.size() == factor * 5;
            assert source2.size() == factor * 5;
            output.collect(
                    new StreamRecord<>(source1.size() + source2.size() + streamRecord.getValue()));
        }
    }

    @Test
    public void testOneInput() throws Exception {
        StreamExecutionEnvironment env = getEnv(false);
        DataStream<Long> source1 =
                env.fromParallelCollection(
                        new NumberSequenceIterator(0L * factor, 5L * factor - 1),
                        BasicTypeInfo.LONG_TYPE_INFO);
        DataStream<Long> source2 =
                env.fromParallelCollection(
                        new NumberSequenceIterator(5L * factor, 10L * factor - 1),
                        BasicTypeInfo.LONG_TYPE_INFO);
        HashMap<String, DataStream<?>> bcStreamsMap = new HashMap<>();
        bcStreamsMap.put("source1", source1);
        bcStreamsMap.put("source2", source2);

        KeyedStream<Long, Long> keyedStream = source2.keyBy(x -> x);

        DataStream<Long> result =
                BroadcastUtils.withBroadcastSet(
                        keyedStream,
                        new OneInputSumOp(),
                        BasicTypeInfo.LONG_TYPE_INFO,
                        bcStreamsMap,
                        true);

        assert count(result) == factor * 5L;
    }

    /** Test class for testTwoInput(). */
    static class TwoInputSumOP extends AbstractStreamOperator<Long>
            implements TwoInputStreamOperator<Long, Long, Long> {

        @Override
        public void processElement1(StreamRecord<Long> streamRecord) throws Exception {
            List<Long> source1 = BroadcastContext.getBroadcastVariable("source1");
            List<Long> source2 = BroadcastContext.getBroadcastVariable("source2");
            assert source1.size() == factor * 5;
            assert source2.size() == factor * 5;
            output.collect(
                    new StreamRecord<>(source1.size() + source2.size() + streamRecord.getValue()));
        }

        @Override
        public void processElement2(StreamRecord<Long> streamRecord) throws Exception {
            List<Long> source1 = BroadcastContext.getBroadcastVariable("source1");
            List<Long> source2 = BroadcastContext.getBroadcastVariable("source2");
            assert source1.size() == factor * 5;
            assert source2.size() == factor * 5;
            output.collect(
                    new StreamRecord<>(source1.size() + source2.size() + streamRecord.getValue()));
        }
    }

    @Test
    public void testTwoInput() throws Exception {
        StreamExecutionEnvironment env = getEnv(false);

        DataStream<Long> source1 =
                env.fromParallelCollection(
                        new NumberSequenceIterator(0L * factor, 5L * factor - 1),
                        BasicTypeInfo.LONG_TYPE_INFO);
        DataStream<Long> source2 =
                env.fromParallelCollection(
                        new NumberSequenceIterator(5L * factor, 10L * factor - 1),
                        BasicTypeInfo.LONG_TYPE_INFO);
        HashMap<String, DataStream<?>> bcStreamsMap = new HashMap<>();
        bcStreamsMap.put("source1", source1);
        bcStreamsMap.put("source2", source2);

        DataStream<Long> result =
                BroadcastUtils.withBroadcastSet(
                        source1,
                        source2,
                        new TwoInputSumOP(),
                        BasicTypeInfo.LONG_TYPE_INFO,
                        bcStreamsMap,
                        new boolean[] {true, true});

        assert count(result) == factor * 10L;
    }

    /** Test class for testMultiInput(). */
    static class SimpleMultiInputStreamOperatorFactory extends AbstractStreamOperatorFactory<Long> {
        int numInputs;

        public SimpleMultiInputStreamOperatorFactory(int numInputs) {
            this.numInputs = numInputs;
        }

        @Override
        public <T extends StreamOperator<Long>> T createStreamOperator(
                StreamOperatorParameters<Long> streamOperatorParameters) {
            return (T) new SimpleMultiInputStreamOperator(streamOperatorParameters, numInputs);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return SimpleMultiInputStreamOperator.class;
        }
    }

    /** Test class for testMultiInput(). */
    static class SimpleMultiInputStreamOperator extends AbstractStreamOperatorV2<Long>
            implements MultipleInputStreamOperator<Long> {
        List<Input> inputList;

        public SimpleMultiInputStreamOperator(
                StreamOperatorParameters<Long> parameters, int numberOfInputs) {
            super(parameters, numberOfInputs);
            this.inputList = new ArrayList<>(numberOfInputs);
            for (int i = 0; i < numberOfInputs; i++) {
                inputList.add(new SumBroadcastInput(this, i + 1));
            }
        }

        @Override
        public List<Input> getInputs() {
            return inputList;
        }

        class SumBroadcastInput extends AbstractInput<Long, Long> {
            StreamRecord<Long> reused;

            public SumBroadcastInput(AbstractStreamOperatorV2<Long> owner, int inputId) {
                super(owner, inputId);
            }

            @Override
            public void processElement(StreamRecord<Long> streamRecord) throws Exception {
                List<Long> source1 = BroadcastContext.getBroadcastVariable("source1");
                List<Long> source2 = BroadcastContext.getBroadcastVariable("source2");
                assert source1.size() == factor * 5;
                assert source2.size() == factor * 5;
                if (null == reused) {
                    reused = new StreamRecord<>(null);
                }
                reused.replace(source1.size() + source2.size() + streamRecord.getValue());
                output.collect(reused);
            }
        }
    }

    @Test
    public void testMultiInput() throws Exception {
        StreamExecutionEnvironment env = getEnv(false);

        DataStream<Long> source1 =
                env.fromParallelCollection(
                        new NumberSequenceIterator(0L * factor, 5L * factor - 1),
                        BasicTypeInfo.LONG_TYPE_INFO);
        DataStream<Long> source2 =
                env.fromParallelCollection(
                        new NumberSequenceIterator(5L * factor, 10L * factor - 1),
                        BasicTypeInfo.LONG_TYPE_INFO);
        HashMap<String, DataStream<?>> bcStreamsMap = new HashMap<>();
        bcStreamsMap.put("source1", source1);
        bcStreamsMap.put("source2", source2);

        SimpleMultiInputStreamOperatorFactory operatorFactory =
                new SimpleMultiInputStreamOperatorFactory(2);

        List<DataStream<?>> inputList = new ArrayList<>();
        inputList.add(source1);
        inputList.add(source2);
        DataStream<Long> result =
                BroadcastUtils.withBroadcastSet(
                        inputList,
                        operatorFactory,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        bcStreamsMap,
                        new boolean[] {true, true});

        assert count(result) == factor * 10L;
    }
}
