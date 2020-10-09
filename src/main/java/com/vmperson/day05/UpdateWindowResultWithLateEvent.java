package com.vmperson.day05;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * @ClassName: UpdateWindowResultWithLateEvent
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/30  9:31
 * @Version: 1.0
 */
public class UpdateWindowResultWithLateEvent {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.socketTextStream("hadoop102", 9999).map(
                r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1]) * 1000L)
        ).returns(new TypeHint<Tuple2<String, Long>>() {
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }))
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                .process(new UpdateWindowResult())
                .print();
        env.execute();
    }

    public static class UpdateWindowResult extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
            Long count = 0L;
            for (Tuple2<String, Long> element : elements) {
                count += 1;
            }

            ValueState<Boolean> isUpdate = context.windowState().getState(new ValueStateDescriptor<Boolean>("isUpdate", Types.BOOLEAN));

            if (isUpdate.value() == null) {
                out.collect("窗口第一次触发计算： " + count + "条数据！");
                isUpdate.update(true);
            } else {
                out.collect("窗口更新了一共有： " + count + "条数据 ！");
            }


        }
    }


}