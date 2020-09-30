package com.vmperson.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: RedirectLateEventToSideOutputExample2
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/29  21:14
 * @Version: 1.0
 */
public class RedirectLateEventToSideOutputExample2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //数据源
        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env.socketTextStream("hadoop102", 9999)
                .map(r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1]) * 1000L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                //设置自增无延迟水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        //指定事件时间字段
                        return element.f1;
                    }
                }));

        //对数据进行分流 处理
        SingleOutputStreamOperator<String> res = stream.keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (value.f1 < ctx.timerService().currentWatermark()) {
                            ctx.output(new OutputTag<String>("late-reading") {
                            }, "时间戳为： " + value.f1 + " 的数据迟到了！");
                        } else {
                            out.collect("时间戳为： " + value.f1 + "  未迟到 ！");
                        }
                    }
                });


        res.print();
        res.getSideOutput(new OutputTag<String>("late-reading"){}).print();

        env.execute();
    }


}