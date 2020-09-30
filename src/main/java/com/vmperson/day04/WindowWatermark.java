package com.vmperson.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName: WindowWatermark
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/29  14:00
 * @Version: 1.0
 */
public class WindowWatermark {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(60 * 1000L);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        source.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String str) throws Exception {
                String[] s = str.split(" ");
                return Tuple2.of(s[0], Long.parseLong(s[1]) * 1000L);
            }
        })
                //默认200毫秒插入一次水位线
                .assignTimestampsAndWatermarks(
                        //为乱序事件流，插入水位线，并设置延迟时间为5s
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        //返回事件时间戳字段
                                        return element.f1;
                                    }
                                })
                ).keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long count = 0L;
                        for (Tuple2<String, Long> i : elements) {
                            count += 1;
                        }
                        out.collect("窗口中共有 " + count + " 条元素");
                    }
                }).print();

        env.execute();
    }


}