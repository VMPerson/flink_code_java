package com.vmperson.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: TwoStreamJoin
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/13  15:37
 * @Version: 1.0
 */
public class TwoStreamJoin {

    private static OutputTag<String> mypayOutTag = new OutputTag<String>("mypayOutTag") {
    };
    private static OutputTag<String> otherpayOutTag = new OutputTag<String>("otherpayOutTag") {
    };


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> myStream = env.fromElements(
                Tuple3.of("order_1", "pay", 1000L),
                Tuple3.of("order_2", "pay", 2000L),
                Tuple3.of("order_4", "pay", 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );


        SingleOutputStreamOperator<Tuple3<String, String, Long>> otherStream = env.fromElements(
                Tuple3.of("order_1", "Wechat", 4000L),
                Tuple3.of("order_3", "ZhifuBao", 5000L),
                Tuple3.of("order_4", "ZhifuBao", 7000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );

        SingleOutputStreamOperator<String> process = myStream.connect(otherStream)
                .keyBy(f -> f.f0, e -> e.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {

                    private ValueState<Tuple3<String, String, Long>> me;
                    private ValueState<Tuple3<String, String, Long>> he;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        me = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("me", Types.TUPLE()));
                        he = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("he", Types.TUPLE()));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (he.value() != null) {
                            out.collect("订单ID为：" + value.f0 + " 实时对账成功！");
                            he.clear();
                        } else {
                            me.update(value);
                            ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (me.value() != null) {
                            out.collect("订单ID为：" + value.f0 + " 实时对账成功！");
                            me.clear();
                        } else {
                            he.update(value);
                            ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
                        }
                    }


                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (me.value() != null) {
                            ctx.output(mypayOutTag, "订单id为： " + me.value().f0 + " 本地未支付");
                            me.clear();
                        }
                        if (he.value() != null) {
                            ctx.output(otherpayOutTag, "订单id为： " + he.value().f0 + " 第三方未支付");
                            he.clear();
                        }
                    }
                });
        process.print();
        process.getSideOutput(mypayOutTag).print();
        process.getSideOutput(otherpayOutTag).print();


        env.execute("双流Join");

    }


}