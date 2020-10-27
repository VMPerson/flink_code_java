package com.vmperson.day10;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: AppMarketingByChannel
 * @Description: TODO APP分渠道1数据统计
 * @Author: VmPerson
 * @Date: 2020/10/14  10:47
 * @Version: 1.0
 */
public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<MarketingUserBehavior> source = env.addSource(new SimulatedEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<MarketingUserBehavior>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<MarketingUserBehavior>() {
                            @Override
                            public long extractTimestamp(MarketingUserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        }
                )).filter(r -> !r.behavior.equals("UNINSTALL"));
        source.map(new MapFunction<MarketingUserBehavior, Tuple2<Tuple2<String, String>, Long>>() {
            @Override
            public Tuple2<Tuple2<String, String>, Long> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(Tuple2.of(value.channel, value.behavior), 1L);
            }
        }).keyBy(new KeySelector<Tuple2<Tuple2<String, String>, Long>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, Long> value) throws Exception {
                return value.f0;
            }
        })
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .process(new MarketingCountByChannel())
                .print();


        env.execute("分渠道数据统计！");
    }

    public static class MarketingCountByChannel extends ProcessWindowFunction<Tuple2<Tuple2<String, String>, Long>, String, Tuple2<String, String>, TimeWindow> {

        @Override
        public void process(Tuple2<String, String> stringStringTuple2, Context context, Iterable<Tuple2<Tuple2<String, String>, Long>> elements, Collector<String> out) throws Exception {
            Long count = 0L;
            for (Tuple2<Tuple2<String, String>, Long> element : elements) {
                count += 1;
            }
            out.collect("渠道和行为是： " + stringStringTuple2.f0 + ":" + stringStringTuple2.f1 + "\t 次数：" + count + " \t 窗口结束时间： " + new Timestamp(context.window().getEnd()));
            count = 0L;
        }


    }


}