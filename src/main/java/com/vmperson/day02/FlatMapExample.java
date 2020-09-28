package com.vmperson.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: FlatMapExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  13:59
 * @Version: 1.0
 */
public class FlatMapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> black = env.fromElements("black green yello write black");

        //通过匿名内部类的方式
  /*      black.flatMap(new FlatMapFunction<String, Map<String, Integer>>() {
            @Override
            public void flatMap(String str, Collector<Map<String, Integer>> out) throws Exception {
                String[] s = str.split(" ");
                for (String word : s) {
                    if (!word.equals("yello")) {
                        HashMap<String, Integer> map = new HashMap<>();
                        map.put(word, 1);
                        out.collect(map);
                    }
                }
            }
        }).print();*/

        //通过自定义类的方式
        black.flatMap(new MyFlatMap()).print();


        env.execute();
    }


}

class MyFlatMap implements FlatMapFunction<String, Map<String, Integer>> {

    @Override
    public void flatMap(String str, Collector<Map<String, Integer>> out) throws Exception {
        String[] s = str.split(" ");
        for (String word : s) {
            if (!word.equals("yello")) {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(word, 1);
                out.collect(map);
            }
        }
    }
}