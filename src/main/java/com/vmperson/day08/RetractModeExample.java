package com.vmperson.day08;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName: RetractModeExample
 * @Description: TODO 重点理解append和retract两者的区别： 追加模式是在没有聚合函数的情况下使用，在有聚合函数的情况下使用retract模式
 * @Author: VmPerson
 * @Date: 2020/10/12  18:18
 * @Version: 1.0
 */
public class RetractModeExample {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取流环境配置信息，创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        //数据源
        DataStreamSource<SensorReading> streamSource = env.addSource(new SensorSource());


        //通过TableAPI的方式：创建表并查询
     /*   Table table = tableEnvironment.fromDataStream(
                streamSource,
                $("sensorId").as("id"),
                $("LongTime").as("ts"),
                $("curFTemp"),
                $("pt").proctime()
        );
        Table select = table
                .groupBy($("id"))
                .select($("id"), $("id").count());
        tableEnvironment.toRetractStream(select, Row.class).print();*/


        //通过FlinkSql的方式
        tableEnvironment.createTemporaryView(
                "sensor",
                streamSource,
                $("sensorId").as("id"),
                $("LongTime").as("ts"),
                $("curFTemp"),
                $("pt").proctime()
        );

        Table query = tableEnvironment.sqlQuery("select id,count(id) from sensor group by id ");
        tableEnvironment.toRetractStream(query, Row.class).print();

        env.execute("表和外部连接器数据转换格式为撤回模式！");
    }


}