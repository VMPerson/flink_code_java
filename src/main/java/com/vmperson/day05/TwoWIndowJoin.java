package com.vmperson.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: TwoWIndowJoin
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/30  21:36
 * @Version: 1.0
 */
public class TwoWIndowJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(Tuple2.of("a", 1000L),
                Tuple2.of("b", 2000L),
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 2000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }));

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env.fromElements(Tuple2.of("a", 3000L),
                Tuple2.of("b", 3000L),
                Tuple2.of("a", 4000L),
                Tuple2.of("b", 4000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }));

        stream1.join(stream2).where(r->r.f0)
                .equalTo(r->r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> first, Tuple2<String, Long> second) throws Exception {
                        return first + "  >>>>>>>>>>>>   "+ second;
                    }
                })
                .print();


        env.execute();
    }


}