package com.vmperson.day03;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: TempIncreaseAlert
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/28  18:52
 * @Version: 1.0
 */
public class TempIncreaseAlert {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> streamSource = env.addSource(new SensorSource());
        streamSource.keyBy(r -> r.sensorId)
                .process(new MyKeyProcessFun())
                .print();

        env.execute();
    }


    public static class MyKeyProcessFun extends KeyedProcessFunction<String, SensorReading, String> {
        private ValueState<Double> lastTemp;                       //最后一次温度值
        private ValueState<Long> currentTimer;                    //最后一次时间戳

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last_temp", Types.DOUBLE));
            currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
        }


        @Override
        public void processElement(SensorReading sensorReading, Context ctx, Collector<String> out) throws Exception {
            //新建一个变量接收 上一次温度 新建一个变量接收上次的时间戳
            Double prevTemp = 0.0;
            Long curTimerTimestamp = 0L;
            if (lastTemp.value() != null) {
                prevTemp = lastTemp.value();
            }
            lastTemp.update(sensorReading.curFTemp);  //设置这次的温度值

            //获取上一次的时间戳
            if (currentTimer.value() != null) {
                curTimerTimestamp = currentTimer.value();
            }


            //判断如果这次是第一次或者温度小于上一次的温度值，则删除定时器，清空定时器变量，
            if (prevTemp == 0.0 || sensorReading.curFTemp < prevTemp) {
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                currentTimer.clear();
            } else if (sensorReading.curFTemp > curTimerTimestamp && curTimerTimestamp == 0L) {
                long onSecondLater = ctx.timerService().currentProcessingTime() + 1000L;
                ctx.timerService().registerProcessingTimeTimer(onSecondLater);
                currentTimer.update(onSecondLater);
            }

        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器为： " + ctx.getCurrentKey() + " 连续一秒内升温！");
            currentTimer.clear();
        }
    }


}