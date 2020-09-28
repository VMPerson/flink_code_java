package com.vmperson.day02;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: MapExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  13:49
 * @Version: 1.0
 */
public class MapExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());

        //使用原算子
        source.map(r -> r.getSensorId()).print();
        //使用匿名内部类的方式
        SingleOutputStreamOperator<String> res = source.map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading str
            ) throws Exception {
                return str.getSensorId();
            }
        });
        res.print();
        //使用自定义类的方式
        source.map(new MyMap()).print();
        env.execute();
    }

}


class MyMap implements MapFunction<SensorReading, String> {

    @Override
    public String map(SensorReading value) throws Exception {
        return value.getSensorId();
    }
}