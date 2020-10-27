package com.vmperson.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: TriggerExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/30  14:59
 * @Version: 1.0
 */
public class TriggerExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value.split(" ")[0], Long.parseLong(value.split(" ")[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }))
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .trigger(new OneSecondIntervalTrigger())
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long count = 0L;
                        for (Tuple2<String, Long> element : elements) {
                            count += 1;
                        }
                        out.collect("窗口中共有： " + count + " 条元素！");
                    }
                }).print();

        env.execute();

    }


    //触发器的自定义实现类
    public static class OneSecondIntervalTrigger extends Trigger<Tuple2<String, Long>, TimeWindow> {

        //每来一条数据执行一次
        @Override
        public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            //定义一个状态变量来保存状态
            ValueState<Boolean> partitionedState = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN));
            if (partitionedState.value() == null) {
                long l = ctx.getCurrentWatermark() + (1000L - ctx.getCurrentWatermark() % 1000L);
                ctx.registerEventTimeTimer(l);
                ctx.registerEventTimeTimer(window.getEnd());
                partitionedState.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        //定时器逻辑
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if (time == window.getEnd()) {
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                System.out.println("当前水位线为： " + ctx.getCurrentWatermark());
                long l = ctx.getCurrentWatermark() + 1000L - ctx.getCurrentWatermark() % 1000L;
                if (l < window.getEnd()) {
                    ctx.registerEventTimeTimer(l);
                }
                return TriggerResult.FIRE;
            }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> partitionedState = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN));
            partitionedState.clear();
        }
    }


}