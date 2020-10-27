package com.vmperson.day08;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName: EventtimeWindowConcept
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/12  14:40
 * @Version: 1.0
 */
public class EventtimeWindowConcept {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        SingleOutputStreamOperator<SensorReading> sensorReadingSingleOutputStreamOperator = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.LongTime;
                    }
                }));



     /*   Table table = tableEnvironment.fromDataStream(
                sensorReadingSingleOutputStreamOperator,
                $("sensorId"),
                $("LongTime").rowtime().as("ts"),
                $("curFTemp")
        );
        //TableAPI 滑动
        table.window(Tumble.over(lit(10).seconds()).on($("ts")).as("win"));
        //TableAPI滚动
        table.window(Slide.over(lit(10).seconds()).every(lit(5).seconds()).on($("ts")).as("win"));*/

        tableEnvironment.createTemporaryView("sensor"
                , sensorReadingSingleOutputStreamOperator,
                $("sensorId").as("id"),
                $("LongTime").rowtime().as("ts"),
                $("curFTemp")
        );
        //FlinkSQL 滚动窗口模式
        Table tableResult1 = tableEnvironment.sqlQuery("select id,count(id) ,Tumble_start(ts,interval '10' second ),Tumble_end(ts ,interval '10' second ) from sensor where id ='source_1' GROUP BY id, TUMBLE(ts, INTERVAL '10' SECOND) ");
        //FlinkSQL 滑动窗口模式
        Table tableResult2 = tableEnvironment.sqlQuery("select id,count(id) ,HOP_start(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND),HOP_end(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) from sensor where id ='source_1' GROUP BY id, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) ");
        tableEnvironment.toRetractStream(tableResult2, Row.class).print();


        env.execute(" 事件时间的滑动窗口和滚动窗口 ！");
    }


}