package com.vmperson.day07;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: CheckPointExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/10  13:51
 * @Version: 1.0
 */
public class CheckPointExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //开启检查点，10s做一次
        env.enableCheckpointing(10 * 1000);
        //保存位置，设置状态后端
        env.setStateBackend(new FsStateBackend("file:///D:\\CodeWorkSpace\\flink_code_java\\src\\main\\resources\\"));

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());
        stream.print();

        env.execute("检查点操作的案例 ");
    }


}