package com.vmperson.day02;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: SensorStream
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  11:41
 * @Version: 1.0
 */
public class SensorStream {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
        sensorReadingDataStreamSource.print();
        env.execute();


    }
}