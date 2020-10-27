package com.vmperson.day10;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;


/**
 * @ClassName: ApacheLogAnaliz
 * @Description: TODO Apache日志解析获取TopN
 * @Author: VmPerson
 * @Date: 2020/10/14  9:23
 * @Version: 1.0
 */
public class ApacheLogAnaliz {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");

        DataStreamSource<String> resourse = env.readTextFile("D:\\CodeWorkSpace\\flink_code_java\\src\\main\\resources\\apachelog.txt");

        SingleOutputStreamOperator<ApacheLogFileds> map = resourse.map(elem -> {
            String[] lines = elem.split(" ");
            ApacheLogFileds apacheLogFileds = new ApacheLogFileds(lines[0], lines[2], format.parse(lines[3]).getTime(), lines[5], lines[6]);
            return apacheLogFileds;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<ApacheLogFileds>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
                new SerializableTimestampAssigner<ApacheLogFileds>() {
                    @Override
                    public long extractTimestamp(ApacheLogFileds element, long recordTimestamp) {
                        return element.eventTime;
                    }
                }
        ));


        map.keyBy(r -> r.url)
                .timeWindow(Time.minutes(1), Time.seconds(5))
                .aggregate(new MyAcc(), new WindowResult())
                .keyBy(r -> r.winEnd)
                .process(new MyKeyPW(3))
                .print();


        env.execute("Apache日志解析！");


    }


    public static class MyAcc implements AggregateFunction<ApacheLogFileds, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogFileds value, Long accumulator) {
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


    public static class WindowResult extends ProcessWindowFunction<Long, ApacheLogCount, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ApacheLogCount> out) throws Exception {
            out.collect(new ApacheLogCount(key, context.window().getEnd(), elements.iterator().next()));
        }
    }

    public static class MyKeyPW extends KeyedProcessFunction<Long, ApacheLogCount, String> {
        //初始化一个状态变量
        private ListState<ApacheLogCount> listState;

        public Integer topN;

        public MyKeyPW() {
        }

        public MyKeyPW(Integer topN) {
            this.topN = topN;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ApacheLogCount>("list_stat", ApacheLogCount.class));
        }

        @Override
        public void processElement(ApacheLogCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.winEnd + 100L);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Iterable<ApacheLogCount> apacheLogCounts = listState.get();
            ArrayList<ApacheLogCount> logCounts = new ArrayList<>();
            for (ApacheLogCount logCount : apacheLogCounts) {
                logCounts.add(logCount);
            }
            listState.clear();
            logCounts.sort(new Comparator<ApacheLogCount>() {
                @Override
                public int compare(ApacheLogCount o1, ApacheLogCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            StringBuilder sb = new StringBuilder();
            sb.append("===========================================\n")
                    .append("time: ")
                    .append(new Timestamp(timestamp - 100L))
                    .append("\n");

            for (int i = 0; i < topN; i++) {
                ApacheLogCount apacheLogCount = logCounts.get(i);
                sb
                        .append("No.")
                        .append(i + 1)
                        .append(" : ")
                        .append(apacheLogCount.url)
                        .append(" count = ")
                        .append(apacheLogCount.count)
                        .append("\n");
            }

            sb
                    .append("===========================================\n\n\n");
            Thread.sleep(1000L);
            out.collect(sb.toString());
        }


        @Override
        public void close() throws Exception {
            super.close();
        }


    }


}