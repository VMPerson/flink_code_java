package com.vmperson.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: OrdertimeoutDetect
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/10  15:12
 * @Version: 1.0
 */
public class OrdertimeoutDetect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<OrderEvent> source = env.fromElements(
                new OrderEvent("00001", "create", 1000L),
                new OrderEvent("00002", "create", 2000L),
                new OrderEvent("00001", "pay", 4000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.eventTime;
                    }
                }));


        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.seconds(5));

        PatternStream<OrderEvent> p = CEP.pattern(source.keyBy(r -> r.orderId), pattern);

        SingleOutputStreamOperator<String> resout = p.select(
                new OutputTag<String>("order-timeOut") {
                },


                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                        return "订单ID为 " + map.get("create").get(0).orderId + " 没有支付！";
                    }
                },
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> map) throws Exception {
                        return "订单ID为 " + map.get("pay").get(0).orderId + " 已经支付！";
                    }
                }

        );

        resout.print();
        resout.getSideOutput(new OutputTag<String>("order-timeOut") {
        }).print();


        env.execute("订单超时提醒");

    }


}