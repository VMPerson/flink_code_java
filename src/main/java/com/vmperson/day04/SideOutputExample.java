package com.vmperson.day04;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: SideOutputExample
 * @Description: TODO 侧输出流
 * @Author: VmPerson
 * @Date: 2020/9/29  19:39
 * @Version: 1.0
 */
public class SideOutputExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //输入数据源 
        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());


        //定义侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("output") {
        };
        OutputTag<String> outputTag1 = new OutputTag<String>("output1") {
        };


  /*      //采用process方式处理
        SingleOutputStreamOperator<SensorReading> stream1 = source.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading str, Context ctx, Collector<SensorReading> out) throws Exception {
                if (str.getCurFTemp() < 32) {
                    ctx.output(outputTag, "id为： " + str.getSensorId() + " 的温度小于32C");
                }
                if (str.getCurFTemp() > 100) {
                    ctx.output(outputTag1, "高温预警！" + str.getSensorId());
                }
                out.collect(str);
            }
        });*/


        //通过分流方式
        SingleOutputStreamOperator<SensorReading> stream1 = source.keyBy(r -> r.getSensorId())
                .process(new KeyedProcessFunction<String, SensorReading, SensorReading>() {

                    @Override
                    public void processElement(SensorReading sensorReading, Context ctx, Collector<SensorReading> out) throws Exception {
                        if (sensorReading.getCurFTemp() < 32) {
                            ctx.output(outputTag, "id: " + sensorReading.getSensorId() + " 温度小于32C！");
                        }
                        if (sensorReading.getCurFTemp() > 100) {
                            ctx.output(outputTag1, "高温预警！ " + sensorReading.getSensorId());
                        }
                        out.collect(sensorReading);
                    }
                });


        //低温预警
        stream1.getSideOutput(outputTag).print();
        //高温预警
        stream1.getSideOutput(outputTag1).print();
        //将所有数据输出
        stream1.print();



        env.execute();
    }

    //我们采用两种方式：两种方式的区别 ：




}