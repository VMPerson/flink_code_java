package com.vmperson.day02;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: ReduceExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  15:01
 * @Version: 1.0
 */
public class ReduceExample {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> ds = env.addSource(new SensorSource());
        ds.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2 map(SensorReading str) throws Exception {
                return Tuple2.of(str.sensorId, str.curFTemp);
            }
        })
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                    @Override
                    public Tuple2 reduce(Tuple2<String, Double> t1, Tuple2<String, Double> t2) throws Exception {
                        if (t1.f1 > t2.f1) {
                            return t1;
                        }else{
                            return t2;
                        }
                    }
                }).print();


        env.execute();

    }


}