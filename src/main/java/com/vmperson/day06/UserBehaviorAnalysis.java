package com.vmperson.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
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
        env.readTextFile("D:\\CodeWorkSpace\\flink_code_java\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String line) throws Exception {
                        String[] split = line.split(",");
                        return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
                    }
                }).filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })).keyBy(r -> r.itemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(r -> r.windowEnd)
                .process(new MyKeyProcess(3))
                .print();

        env.execute("实时TopN");
    }


    //增量聚合窗口:计算每个商品的累加值 CountAgg
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

    //全量聚合窗口: 补全窗口信息 WindowResult
    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(key, elements.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }

    //根据窗口关闭时间分流之后，进行排序求值操作 MyKeyProcess

    public static class MyKeyProcess extends KeyedProcessFunction<Long, ItemViewCount, String> {
        //初始化一个临时状态变量
        private ListState<ItemViewCount> itemStat;

        //前几名
        public Integer topN;

        public MyKeyProcess() {
        }

        public MyKeyProcess(Integer topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemStat = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-stat", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemStat.add(value);
            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            //排序
            ArrayList<ItemViewCount> itemViewCounts = new ArrayList<>();
            itemStat.get().forEach(r->itemViewCounts.add(r));
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.count.intValue()-o1.count.intValue();
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