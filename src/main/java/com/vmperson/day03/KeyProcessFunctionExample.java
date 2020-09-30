package com.vmperson.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: KeyProcessFunctionExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/28  16:20
 * @Version: 1.0
 */
public class KeyProcessFunctionExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        source.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String str, Collector<Tuple2<String, String>> out) throws Exception {
                String[] s = str.split(" ");
                out.collect(Tuple2.of(s[0], s[1]));
            }
        }).keyBy(r -> r.f0)
                .process(new KeyByProcess())
                .print();

        env.execute();
    }


    public static class KeyByProcess extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
            long l = ctx.timerService().currentProcessingTime() + 10 * 1000;
            ctx.timerService().registerEventTimeTimer(l);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器触发了！");
        }
    }


}