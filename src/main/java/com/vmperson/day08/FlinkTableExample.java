package com.vmperson.day08;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;


/**
 * @ClassName: FlinkTableExample
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/12  9:59
 * @Version: 1.0
 */
public class FlinkTableExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> streamSource = env.addSource(new SensorSource());


        //使用TableAPI的方式
        Table table1 = tableEnv.fromDataStream(
                streamSource,
                $("sensorId"),
                $("LongTime").as("ts"),
                $("curFTemp"),
                $("pt").proctime()
        );

        Table select = table1.window(Tumble.over(lit(10).seconds()).on($("pt")).as("win"))
                .groupBy($("sensorId"), $("win"))
                .select($("sensorId"), $("sensorId").count());
        //     tableEnv.toRetractStream(select, Row.class).print();

        //使用FlinkSQL的方式
        tableEnv.createTemporaryView(
                "sensor",
                streamSource,
                $("sensorId"),
                $("LongTime").as("ts"),
                $("curFTemp"),
                $("pt").proctime());

        Table table = tableEnv.sqlQuery(" select sensorId ,count(sensorId) from sensor group by sensorId ,tumble(pt,interval '10' second)");

        tableEnv.toRetractStream(table, Row.class).print();


        env.execute(" TAbleAPI和FlinkSQL的练习！");
    }


}