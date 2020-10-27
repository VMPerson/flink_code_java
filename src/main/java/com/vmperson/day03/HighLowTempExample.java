package com.vmperson.day03;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: HighLowTempExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/28  14:29
 * @Version: 1.0
 */
public class HighLowTempExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> streamSource = env.addSource(new SensorSource());

        streamSource.keyBy(r -> r.sensorId)
                .timeWindow(Time.seconds(5))
                .aggregate(new AvgFun(), new WindowFun())
                .print();
        env.execute();
    }



    public static class WindowFun extends ProcessWindowFunction<Tuple3<String, Double, Double>,HighLowTemp,String, TimeWindow>{
        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Double, Double>> elements, Collector<HighLowTemp> out) throws Exception {
            Tuple3<String, Double, Double> tmp = elements.iterator().next();
            out.collect(new HighLowTemp(key,tmp.f1,tmp.f2,context.window().getStart(),context.window().getEnd()));
        }
    }


    public static class AvgFun implements AggregateFunction<SensorReading, Tuple3<String, Double, Double>, Tuple3<String, Double, Double>> {
        @Override
        public Tuple3<String, Double, Double> createAccumulator() {
            return Tuple3.of("", Double.MAX_VALUE, Double.MIN_VALUE);
        }
        @Override
        public Tuple3<String, Double, Double> add(SensorReading r1, Tuple3<String, Double, Double> accumulator) {

            return Tuple3.of(r1.sensorId, Math.min(r1.curFTemp, accumulator.f1), Math.max(r1.curFTemp, accumulator.f2));
        }
        @Override
        public Tuple3<String, Double, Double> getResult(Tuple3<String, Double, Double> accumulator) {
            return accumulator;
        }
        @Override
        public Tuple3<String, Double, Double> merge(Tuple3<String, Double, Double> a, Tuple3<String, Double, Double> b) {
            return null;
        }
    }


}