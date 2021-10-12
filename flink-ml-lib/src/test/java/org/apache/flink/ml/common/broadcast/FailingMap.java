package org.apache.flink.ml.common.broadcast;

import org.apache.flink.api.common.functions.RichMapFunction;

public class FailingMap extends RichMapFunction<Long, Long> {

    private final int failingCount;

    private int count;

    public FailingMap(int failingCount) {
        this.failingCount = failingCount;
    }

    @Override
    public Long map(Long value) throws Exception {
        count++;
        if (getRuntimeContext().getIndexOfThisSubtask() == 0
                && getRuntimeContext().getAttemptNumber() < 5
                && count >= failingCount) {
            //throw new RuntimeException("failling map");
        }

        return value;
    }
}
