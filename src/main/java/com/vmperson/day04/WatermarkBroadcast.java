package com.vmperson.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WatermarkBroadcast
 * @Description: TODO 双流union
 * @Author: VmPerson
 * @Date: 2020/9/29  18:32
 * @Version: 1.0
 */
public class WatermarkBroadcast{


        public static void main(String[] args) throws Exception {

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            //设置事件时间语义
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


            //数据流1
            SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.socketTextStream("hadoop102", 9999).map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    return Tuple2.of(value.split(" ")[0], Long.parseLong(value.split(" ")[1]) * 1000L);
                }
            })
                    //设置水位线和时间戳
                    .assignTimestampsAndWatermarks(
                            //设置水位线策略
                            WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().
                                    withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                        //设置事件时间是哪个字段
                                        @Override
                                        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                            return element.f1;
                                        }
                                    }));


            //数据流2
            SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env.socketTextStream("hadoop102", 9998).map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    return Tuple2.of(value.split(" ")[0], Long.parseLong(value.split(" ")[1]) * 1000L);
                }
            })
                    //设置水位线和时间戳
                    .assignTimestampsAndWatermarks(
                            //设置水位线策略
                            WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().
                                    withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                        //设置事件时间是哪个字段
                                        @Override
                                        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                            return element.f1;
                                        }
                                    }));

            //双流合并
            stream1.union(stream2).keyBy(r -> r.f0).process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                @Override
                public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                    out.collect("当前水位线为： " + ctx.timerService().currentWatermark());
                }
            }).print();

            env.execute();
        }

}