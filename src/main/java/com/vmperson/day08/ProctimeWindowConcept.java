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
 * @ClassName: ProctimeWindowConcept
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/12  15:06
 * @Version: 1.0
 */
public class ProctimeWindowConcept {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> streamSource = env.addSource(new SensorSource());


        //TableAPI方式
        Table table = tableEnvironment.fromDataStream(
                streamSource,
                $("sensorId").as("id"),
                $("LongTime").as("ts"),
                $("curFTemp"),
                $("pt").proctime()
        );

     /*   table.window(Tumble.over(lit(10).seconds()).on($("id")).as("win"));
        table.window(Slide.over(lit(10).seconds()).every(lit(5).seconds()).on($("id")).as("win"));
*/


        //FlinkSQL方式
        tableEnvironment.createTemporaryView("sensor",
                streamSource,
                $("sensorId").as("id"),
                $("LongTime").as("ts"),
                $("curFTemp"),
                $("pt").proctime()
        );

        Table tableResult1 = tableEnvironment.sqlQuery(" select id, count(id),Tumble_start(pt,Interval '10' second ),Tumble_end(pt,Interval '10' second ) from sensor where id='source_1' group by id ,Tumble(pt,Interval '10' SECOND ) ");
        //  tableEnvironment.toRetractStream(tableResult1, Row.class).print();

        Table tableResult2 = tableEnvironment.sqlQuery(" select id, count(id),HOP_start(pt, INTERVAL '5' SECOND,Interval '10' second ),HOP_end(pt, INTERVAL '5' SECOND,Interval '10' second ) from sensor where id='source_1' group by id ,HOP(pt, INTERVAL '5' SECOND,Interval '10' second ) ");
        tableEnvironment.toRetractStream(tableResult2, Row.class).print();


        env.execute("处理时间滑动滚动窗口练习！");

    }


}