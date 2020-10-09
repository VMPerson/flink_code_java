package com.vmperson.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName: IntervalJoinExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/30  11:11
 * @Version: 1.0
 */
public class IntervalJoinExample {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置事件语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<Tuple3<String, Long, String>, String> stream1 = env.fromElements(Tuple3.of("user_1", 10 * 60 * 1000L, "click"),
                Tuple3.of("user_1", 16 * 60 * 1000L, "click"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, String>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
                        return element.f1;
                    }
                })).keyBy(r -> r.f0);


        KeyedStream<Tuple3<String, Long, String>, String> stream2 = env.fromElements(Tuple3.of("user_1", 5 * 60 * 1000L, "brower"), Tuple3.of("user_1", 6 * 60 * 1000L, "brower"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, String>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
                        return element.f1;
                    }
                })).keyBy(r -> r.f0);


        stream1.intervalJoin(stream2).between(Time.minutes(-10), Time.minutes(0)).process(new ProcessJoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, String>() {
            @Override
            public void processElement(Tuple3<String, Long, String> left, Tuple3<String, Long, String> right, Context ctx, Collector<String> out) throws Exception {
                out.collect(left + " >>>>>  " + right);
            }
        }).print();


        env.execute();
    }


}