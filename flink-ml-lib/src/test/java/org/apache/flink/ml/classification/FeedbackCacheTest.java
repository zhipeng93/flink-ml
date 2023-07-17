package org.apache.flink.ml.classification;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.iteration.TerminateOnMaxIter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.Collections;

public class FeedbackCacheTest {
    @Test
    public void testIteration() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Long> input =
                env.fromCollection(Collections.singletonList(1L)).map(x -> x, Types.LONG);
        DataStream<Long> feedback =
                env.fromCollection(Collections.singletonList(1L)).map(x -> x, Types.LONG);

        DataStreamList resultList =
                Iterations.iterateBoundedStreamsUntilTermination(
                        DataStreamList.of(feedback),
                        ReplayableDataStreamList.notReplay(input),
                        IterationConfig.newBuilder().build(),
                        new LargeFeedbackIterationBody());

        DataStream<Long> result = resultList.get(0);
        result.addSink(
                new SinkFunction<Long>() {
                    @Override
                    public void invoke(Long value) throws Exception {
                        SinkFunction.super.invoke(value);
                    }
                });

        env.execute();
    }

    private static class LargeFeedbackIterationBody implements IterationBody {
        @Override
        public IterationBodyResult process(
                DataStreamList variableStreams, DataStreamList dataStreams) {
            DataStream<Long> feedback = variableStreams.get(0);
            DataStream<Long> input = dataStreams.get(0);
            SingleOutputStreamOperator<Long> newUpdates =
                    feedback.connect(input)
                            .transform("CCLoop", Types.LONG, new LargeFeedbackLoop());

            DataStream<Long> termination = newUpdates.flatMap(new TerminateOnMaxIter(1));

            return new IterationBodyResult(
                    DataStreamList.of(newUpdates), DataStreamList.of(newUpdates), termination);
        }
    }

    private static class LargeFeedbackLoop extends AbstractStreamOperator<Long>
            implements TwoInputStreamOperator<Long, Long, Long>, IterationListener<Long> {

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<Long> collector) {
            for (int i = 0; i < 1000000000; i++) {
                collector.collect((long) i);
                if (i % 10000 == 0) {
                    System.out.printf("Output %d records.\n", i);
                }
            }
        }

        @Override
        public void onIterationTerminated(Context context, Collector<Long> collector) {}

        @Override
        public void processElement1(StreamRecord<Long> streamRecord) {}

        @Override
        public void processElement2(StreamRecord<Long> streamRecord) {}
    }
}
