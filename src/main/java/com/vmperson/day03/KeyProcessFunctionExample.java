package com.vmperson.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: KeyProcessFunctionExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/28  16:20
 * @Version: 1.0
 */
public class KeyProcessFunctionExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        env.execute();
    }


}