package com.vmperson.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: LoginFailDetectWithoutCEP
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/10  16:09
 * @Version: 1.0
 */
public class LoginFailDetectWithoutCEP {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        SingleOutputStreamOperator<LoginEvent> source = env.fromElements(
                new LoginEvent("001", "192.168.1.102", "fail", 1000L),
                new LoginEvent("001", "192.168.1.103", "fail", 2000L),
                new LoginEvent("001", "192.168.1.104", "fail", 4000L),
                new LoginEvent("001", "192.168.1.105", "fail", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.eventTime;
                    }
                }
        ));


        source.keyBy(r -> r.userId)
                .process(new MykeyProcessLogin())
                .print();


        env.execute("");

    }


    public static class MykeyProcessLogin extends KeyedProcessFunction<String, LoginEvent, String> {

        private ListState<LoginEvent> listStates;
        private ValueState<Long> tsTimer;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listStates = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list-state", LoginEvent.class));
            tsTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-Timer", Types.LONG));
        }

        @Override
        public void processElement(LoginEvent loginEvent, Context ctx, Collector<String> out) throws Exception {
            if (loginEvent.eventType.equals("success")) {
                listStates.clear();
                if (tsTimer.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(tsTimer.value());
                    tsTimer.clear();
                }
            } else {
                if (tsTimer.value() == null) {
                    listStates.add(loginEvent);
                    //注册定时器
                    ctx.timerService().registerEventTimeTimer(loginEvent.eventTime + 5000L);
                }
            }
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            if (listStates.get()!=null){
                long count=0;
                for (LoginEvent loginEvent : listStates.get()) {
                    count+=1;
                }
                if (count>2){
                    out.collect("用户ID为： " + ctx.getCurrentKey() + " 的用户恶意登录！");
                }
            }

            listStates.clear();
            tsTimer.clear();

        }
    }


}