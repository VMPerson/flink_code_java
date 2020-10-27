package com.vmperson.day03;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: AvgAggregateFuncation
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/28  11:37
 * @Version: 1.0
 */
public class AvgAggregateFuncation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());

        source.keyBy(r -> r.sensorId)
                .timeWindow(Time.seconds(5))
                .aggregate(new AvgAggregate())
                .print();


        env.execute();

    }

    public static class AvgAggregate implements AggregateFunction<SensorReading, Tuple3<String, Double, Long>, Tuple2<String, Double>> {


        @Override
        public Tuple3<String, Double, Long> createAccumulator() {
            return Tuple3.of("", 0.0, 0L);
        }

        @Override
        public Tuple3<String, Double, Long> add(SensorReading value, Tuple3<String, Double, Long> accumulator) {
            return Tuple3.of(value.sensorId, value.curFTemp + accumulator.f1, accumulator.f2 += 1);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Long> accumulator) {
            return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
        }

        @Override
        public Tuple3<String, Double, Long> merge(Tuple3<String, Double, Long> a, Tuple3<String, Double, Long> b) {
            return null;
        }
    }


}
