package com.vmperson.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: OrdertimeoutDetectWithOutCEP
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/10  16:09
 * @Version: 1.0
 */
public class OrdertimeoutDetectWithOutCEP {

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

        source.keyBy(r -> r.orderId)
                .process(new MyKeyedOrder())
                .print();

        env.execute("");

    }


    public static class MyKeyedOrder extends KeyedProcessFunction<String, OrderEvent, String> {

        private ValueState<OrderEvent> orderStat;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            orderStat = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("orderStat", OrderEvent.class));
        }

        @Override
        public void processElement(OrderEvent orderEvent, Context ctx, Collector<String> out) throws Exception {

            if (orderEvent.eventType.equals("create")) {
                if (orderStat.value() == null) {
                    orderStat.update(orderEvent);
                    ctx.timerService().registerEventTimeTimer(orderEvent.eventTime + 5000L);
                }
            } else {
                orderStat.update(orderEvent);
                out.collect(" 订单ID为： " + orderEvent.orderId + " 支付成功！");
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            if (orderStat.value() != null && orderStat.value().eventType.equals("create")) {
                out.collect(" 订单ID为： " + ctx.getCurrentKey() + " 未支付！");
            }
            orderStat.clear();
        }
    }


}