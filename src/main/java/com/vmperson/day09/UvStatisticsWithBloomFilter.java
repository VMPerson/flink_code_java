package com.vmperson.day09;

import com.vmperson.day06.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator4.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.curator4.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: UvStatisticsWithBloomFilter
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/13  11:59
 * @Version: 1.0
 */
public class UvStatisticsWithBloomFilter {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> resourceStream = env.readTextFile("D:\\CodeWorkSpace\\flink_code_java\\src\\main\\resources\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> uv = resourceStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
            }
        }).filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }
                ));


        uv.map(new MapFunction<UserBehavior, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(UserBehavior value) throws Exception {
                return Tuple2.of("dummy", value.userId);
            }
        }).keyBy(r -> r.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute("使用布隆过滤器去重！每小时的用户活跃量！");


    }


    public static class CountAgg implements AggregateFunction<Tuple2<String, String>, Tuple2<BloomFilter<Long>, Long>, Long> {


        @Override
        public Tuple2<BloomFilter<Long>, Long> createAccumulator() {
            return Tuple2.of(BloomFilter.create(Funnels.longFunnel(), 1000000, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> add(Tuple2<String, String> value, Tuple2<BloomFilter<Long>, Long> accumulator) {

            if (!accumulator.f0.mightContain(Long.parseLong(value.f1))) {
                accumulator.f0.put(Long.parseLong(value.f1));
                accumulator.f1 += 1;
            }

            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<Long>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> merge(Tuple2<BloomFilter<Long>, Long> a, Tuple2<BloomFilter<Long>, Long> b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect("窗口结束时间：" + new Timestamp(context.window().getEnd()) + " 的UV活跃量为：" + elements.iterator().next());
        }
    }


}