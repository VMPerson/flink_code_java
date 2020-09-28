package com.vmperson.day03;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: AggregateFuncationExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/28  11:29
 * @Version: 1.0
 */
public class AggregateFuncationExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //分组----开窗----聚合
        DataStreamSource<SensorReading> resource = env.addSource(new SensorSource());
        resource.keyBy(r->r.getSensorId())
                .timeWindow(Time.seconds(5))
                .aggregate(new MyAggregatte())
                .print();


        env.execute();

    }

    public  static class MyAggregatte implements AggregateFunction<SensorReading,Tuple2<String,Double>,Tuple2<String,Double>>{

        @Override
        public Tuple2<String, Double> createAccumulator() {
            return Tuple2.of("",Double.MAX_VALUE);
        }

        @Override
        public Tuple2<String, Double> add(SensorReading sen, Tuple2<String, Double> acc) {
            if (sen.getCurFTemp()<acc.f1){
                return Tuple2.of(sen.getSensorId(),sen.getCurFTemp());
            }else{
                return acc;
            }
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple2<String, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Double> merge(Tuple2<String, Double> a, Tuple2<String, Double> b) {
            return null;
        }
    }


}