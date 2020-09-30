package com.vmperson.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: RedirectLateEventToSideOutputExample1
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/29  20:48
 * @Version: 1.0
 */
public class RedirectLateEventToSideOutputExample1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env.socketTextStream("hadoop102", 9999).map(r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1]) * 1000L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                //设置升序时间戳抽取 不设置延迟时间
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        //指定时间戳字段
                        return element.f1;
                    }
                }));

        //  分流 开窗 处理
        SingleOutputStreamOperator<String> res = stream.keyBy(r -> r.f0)
                .timeWindow(Time.seconds(10))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late-reading") {
                })
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    //全局窗口
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long count = 0L;
                        for (Tuple2<String, Long> stringLongTuple2 : elements) {
                            count += 1;
                        }
                        out.collect("开窗时间： " + context.window().getStart() + " 闭窗时间：" + context.window().getEnd() +
                                "水位线： " + context.currentWatermark() + " 窗口数据条数： " + count);
                    }
                });


        //打印结果
        res.print();
        env.execute();
    }


}