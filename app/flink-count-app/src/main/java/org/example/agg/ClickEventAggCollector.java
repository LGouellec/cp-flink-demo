package org.example.agg;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.flink.aggregated.ClickEventAgg;

import java.util.Date;

public class ClickEventAggCollector  extends ProcessWindowFunction<Long, ClickEventAgg, CharSequence, TimeWindow> {

    @Override
    public void process(CharSequence category,
                        ProcessWindowFunction<Long, ClickEventAgg, CharSequence, TimeWindow>.Context context,
                        Iterable<Long> iterable, Collector<ClickEventAgg> collector) {
        Long count = iterable.iterator().next();

        collector.collect(
                new ClickEventAgg(
                        new Date(context.window().getStart()).toInstant(),
                        new Date(context.window().getEnd()).toInstant(),
                        category,
                        count));
    }
}
