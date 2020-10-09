package com.vmperson.day02;

import com.vmperson.day02.util.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @ClassName: SourceList
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/4  15:54
 * @Version: 1.0
 */
public class SourceList {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

      ArrayList<SensorReading> sensorReadings = new ArrayList<>();
        sensorReadings.add(new SensorReading("001", 100L, 89.32D));
        sensorReadings.add(new SensorReading("002", 87L, 10.2D));
        sensorReadings.add(new SensorReading("003", 12L, 45.1D));
        sensorReadings.add(new SensorReading("004", 132L, 67.21D));


        DataStreamSource<String> ensorReadings = env.readTextFile("D:\\CodeWorkSpace\\flink_code_java\\src\\main\\resources\\sensor.txt");
        DataStreamSource<SensorReading> streamSource = env.fromCollection(sensorReadings);

        streamSource.print();
        env.execute("Test Source List");

    }


}