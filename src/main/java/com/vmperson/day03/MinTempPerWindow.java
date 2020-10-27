package com.vmperson.day03;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: MinTempPerWindow
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/28  10:29
 * @Version: 1.0
 */
public class MinTempPerWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());
        source.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2 map(SensorReading value) throws Exception {
                return Tuple2.of(value.sensorId, value.curFTemp);
            }
        })
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
                        if (value1.f1 > value2.f1) {
                            return value2;
                        } else {
                            return value1;
                        }
                    }
                }).print();
        env.execute();
    }


}