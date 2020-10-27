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
 * @ClassName: AppendModeExample
 * @Description: TODO 对于流式查询，将表和外部连接器之间进行转换，交换的消息的类型模式由updatemode决定，有两种： append追加模式、retract撤回模式、upsert更新插入模式
 * append和retract模式两者的区别： 如果有聚合函数使用 retract,无聚合函数使用append模式
 * @Author: VmPerson
 * @Date: 2020/10/12  18:05
 * @Version: 1.0
 */
public class AppendModeExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> streamSource = env.addSource(new SensorSource());

        tableEnvironment.createTemporaryView("sensor",
                streamSource,
                $("sensorId").as("id"),
                $("LongTime").as("ts"),
                $("curFTemp"),
                $("pt").proctime()
        );
        Table table = tableEnvironment.sqlQuery("select  id from sensor");
        tableEnvironment.toAppendStream(table, Row.class).print();

        env.execute("更新模式！Append追加模式！");
    }


}