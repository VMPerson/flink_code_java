package com.vmperson.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: FilterExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  13:52
 * @Version: 1.0
 */
public class FilterExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.fromElements("black", "write", "green", "black", "black");
        //使用算子方式
   //     source.filter(r->r.equals("black")).print();
        //使用匿名内部类的方式
        SingleOutputStreamOperator<String> black = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (value.equals("black")) {
                    return true;
                } else {
                    return false;
                }
            }
        });
     //   black.print();
        //使用自定义类的方式
        source.filter(new MyFilter()).print();

        env.execute();

    }


}

class MyFilter implements FilterFunction<String>{

    @Override
    public boolean filter(String value) throws Exception {
        if (value.equals("black")) {
            return true;
        } else {
            return false;
        }
    }
}