package com.vmperson.day01;

import akka.stream.impl.fusing.Collect;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/25  14:41
 * @Version: 1.0
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<String> ds = executionEnvironment.socketTextStream("hadoop102", 9999, "\n");
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] array = s.split(" ");
                for (String word : array) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(r -> r.f0).sum(1);
        res.print();

        executionEnvironment.execute("First Code");
    }


}