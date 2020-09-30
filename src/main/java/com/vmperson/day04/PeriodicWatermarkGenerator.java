package com.vmperson.day04;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Timestamp;

/**
 * @ClassName: PeriodicWatermarkGenerator
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/29  20:18
 * @Version: 1.0
 */
public class PeriodicWatermarkGenerator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //生成数据源
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        //自定义水位线变化频率
        source
                .map(r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1]) * 1000L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

                    final long bound = 5 * 1000L;
                    long maxts = Long.MIN_VALUE + bound + 1;

                    //系统在插入水位线的时候执行，默认200ms执行一次，我们自定义修改
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxts - bound - 1);
                    }

                    //每来一条数据执行一次
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        //更新最大事件时间
                        maxts=   Math.max(maxts, element.f1);
                        //指定事件时间字段
                        return element.f1;
                    }
                }).keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> s, Context ctx, Collector<String> out) throws Exception {
                        //针对每一条数据设置事件定时器
                        ctx.timerService().registerEventTimeTimer(s.f1 + 10 * 1000L);
                    }

                    //定时器执行
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("时间戳为： " + new Timestamp(timestamp) + " 的定时器触发了 ！");

                    }
                })
                .print();

        env.execute();
    }


}