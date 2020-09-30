package com.vmperson.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName: WaterMarkTest
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/29  11:43
 * @Version: 1.0
 */
public class WaterMarkTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
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
                .process(new Keyed())
                .print();


        env.execute();
    }

    public static class Keyed extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {
        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            out.collect("当前水位线是：" + ctx.timerService().currentWatermark());
        }
    }


}