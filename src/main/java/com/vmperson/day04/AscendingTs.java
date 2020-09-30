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

import java.sql.Timestamp;

/**
 * @ClassName: AscendingTs
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/29  16:20
 * @Version: 1.0
 */
public class AscendingTs {

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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }
                )).keyBy(r -> r.f0)
                .process(new Keyed())
                .print();

        env.execute();

    }

    public static class Keyed extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            ctx.timerService().registerEventTimeTimer(value.f1 + 10 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("时间戳为： " + new Timestamp(timestamp) + " 的定时器触发了");
        }
    }


}