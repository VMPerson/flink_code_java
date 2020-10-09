package com.vmperson.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @ClassName: UserBehaviorAnalysis
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/9  11:45
 * @Version: 1.0
 */
public class UserBehaviorAnalysis {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置事件语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //读取数据
        SingleOutputStreamOperator<UserBehavior> pv = env.readTextFile("D:\\CodeWorkSpace\\flink_code_java\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String line) throws Exception {
                        String[] splits = line.split(",");
                        return new UserBehavior(splits[0], splits[1], splits[2], splits[3], Long.parseLong(splits[4]) * 1000L);
                    }
                })
                //过滤pv的， 指定事件时间字段
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));


        pv.keyBy(r -> r.itemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(r -> r.windowEnd)
                .process(new MyKeyProcess(3))
                .print();


        env.execute("实时TopN");
    }


    //增量聚合窗口:计算每个商品的累加值
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    //全量聚合窗口: 补全窗口信息
    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(key, elements.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }


    //根据窗口关闭时间分流之后，进行排序求值操作
    public static class MyKeyProcess extends KeyedProcessFunction<Long, ItemViewCount, String> {
        //维护一个状态列表，存储topN
        private ListState<ItemViewCount> itemstae;
        //第几名，
        private Integer topN;

        public MyKeyProcess() {
        }

        //第几名的构造器
        public MyKeyProcess(Integer topN) {
            this.topN = topN;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemstae = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-state", ItemViewCount.class));
        }

        //每来一条数据，添加一下数据状态 ，注册定时器
        @Override
        public void processElement(ItemViewCount itemViewCount, Context ctx, Collector<String> out) throws Exception {
            itemstae.add(itemViewCount);
            ctx.timerService().registerEventTimeTimer(itemViewCount.windowEnd + 100L);
        }

        //触发定时器操作后  ： 排序
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            ArrayList<ItemViewCount> itemViewCounts = new ArrayList<>();
            itemstae.get().iterator().forEachRemaining(r -> itemViewCounts.add(r));

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });
            StringBuilder result = new StringBuilder();
            result
                    .append("===========================================\n")
                    .append("time: ")
                    .append(new Timestamp(timestamp - 100L))
                    .append("\n");

            for (int i = 0; i < topN; i++) {
                ItemViewCount currItem = itemViewCounts.get(i);
                result
                        .append("No.")
                        .append(i + 1)
                        .append(" : ")
                        .append(currItem.itemId)
                        .append(" count = ")
                        .append(currItem.count)
                        .append("\n");
            }
            result
                    .append("===========================================\n\n\n");
            Thread.sleep(1000L);
            out.collect(result.toString());
        }
    }


}