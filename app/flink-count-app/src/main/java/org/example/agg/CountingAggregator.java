package org.example.agg;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.avro.ClickEvent;

public class CountingAggregator implements AggregateFunction<ClickEvent, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ClickEvent value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
