package com.vmperson.day04;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: SwitchSensorStream
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/29  9:31
 * @Version: 1.0
 */
public class SwitchSensorStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<SensorReading, String> stream = env.addSource(new SensorSource()).keyBy(r -> r.getSensorId());
        KeyedStream<Tuple2<String, Long>, String> switchStream = env.fromElements(Tuple2.of("source_1", 10 * 1000L)).keyBy(r -> r.f0);

        stream.connect(switchStream).process(new SwitchFilter()).print();


        env.execute();
    }


    public static class SwitchFilter extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {
        //创建状态变量
        private ValueState<Boolean> isTrue;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            isTrue = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("switch", Types.BOOLEAN));
        }

        //第一条流来了的处理
        @Override
        public void processElement1(SensorReading sen, Context ctx, Collector<SensorReading> out) throws Exception {

            if (isTrue.value() != null && isTrue.value() == true) {
                out.collect(sen);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> t2, Context ctx, Collector<SensorReading> out) throws Exception {
            isTrue.update(true);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + t2.f1);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            isTrue.clear();
        }
    }


}