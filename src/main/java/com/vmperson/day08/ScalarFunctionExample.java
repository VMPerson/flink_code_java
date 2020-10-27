package com.vmperson.day08;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName: ScalarFunctionExample
 * @Description: TODO 标量型自定义函数
 * @Author: VmPerson
 * @Date: 2020/10/12  18:34
 * @Version: 1.0
 */
public class ScalarFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        //添加数据源
        DataStreamSource<SensorReading> streamSource = env.addSource(new SensorSource());

        //注册UDF标量型函数
        tableEnvironment.createFunction("hashFun", HashCodeFunction.class);
        tableEnvironment.getConfig().addJobParameter("hashcode_factor", "31");

        //使用FlinkSQL的方式实现标量型自定义函数
        tableEnvironment.createTemporaryView(
                "sensor",
                streamSource,
                $("sensorId").as("id"),
                $("LongTime").as("ts"),
                $("curFTemp"),
                $("pt").proctime()
        );

        Table table = tableEnvironment.sqlQuery("select id,hashFun(id) from sensor");
        tableEnvironment.toAppendStream(table, Row.class).print();

        env.execute("用户自定义标量函数！");
    }


    //注意：标量型udf函数必须实现eval方法
    public static class HashCodeFunction extends ScalarFunction {

        private Integer factor = 0;

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            factor = Integer.parseInt(context.getJobParameter("hashcode_factor", "12"));
        }

        public Integer eval(String s) {
            return s.hashCode() * factor;
        }


    }


}