package com.vmperson.day03;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @ClassName: ProcessTimeWindow
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/28  14:03
 * @Version: 1.0
 */
public class ProcessTimeWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());
        source.keyBy(r->r.sensorId)
                .timeWindow(Time.seconds(5))
                .process(new MyProcessWindow())
                .print();

        env.execute();

    }

    public static class  MyProcessWindow extends ProcessWindowFunction<SensorReading,String,String, TimeWindow>{


        @Override
        public void process(String s, Context context, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
             Double sum = 0.0;
             Long times =0L;

            for (SensorReading reading : elements) {
                sum+=reading.curFTemp;
                times+=1;
            }

            out.collect("传感器为：" + s + " 窗口结束时间为：" + new Timestamp(context.window().getEnd()) + " 的平均值是：" + sum / times);
        }
    }





}