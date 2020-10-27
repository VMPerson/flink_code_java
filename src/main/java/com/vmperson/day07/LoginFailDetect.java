package com.vmperson.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;


/**
 * @ClassName: LoginFailDetect
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/10  13:59
 * @Version: 1.0
 */
public class LoginFailDetect {
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


        //定义模板 ：方式一
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.eventType.equals("fail");
            }
        })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("three")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .within(Time.seconds(5));


        //定义模板：方式二
        Pattern<LoginEvent, LoginEvent> p2 = Pattern.<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .times(3)
                .within(Time.seconds(5));


        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(r -> r.userId), pattern);
      //  PatternStream<LoginEvent> patternStream2 = CEP.pattern(source.keyBy(r -> r.userId), p2);

        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent first = map.get("first").iterator().next();
                LoginEvent second = map.get("second").iterator().next();
                LoginEvent three = map.get("three").iterator().next();

                return first.ipAddr + " , " + second.ipAddr + " , " + three.ipAddr;
            }
        })
        .print();
/*
        patternStream2.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                for (LoginEvent event : map.get("first")) {
                    System.out.println(event.ipAddr + " ");
                }
                return " Hello World";
            }
        }).print();*/

        env.execute("测试CEP复杂流处理！做风控，恶意登陆，连续登陆！");
    }


}