package com.vmperson.day02;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: UnionExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  19:48
 * @Version: 1.0
 */
public class UnionExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> source_1 = env.addSource(new SensorSource()).filter(r -> r.getSensorId().equals("source_1"));
        SingleOutputStreamOperator<SensorReading> source_2 = env.addSource(new SensorSource()).filter(r -> r.getSensorId().equals("source_2"));
        SingleOutputStreamOperator<SensorReading> source_3 = env.addSource(new SensorSource()).filter(r -> r.getSensorId().equals("source_3"));

        DataStream<SensorReading> res = source_1.union(source_2, source_3);
        res.print();

        env.execute();

    }


}