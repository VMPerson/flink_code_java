package com.vmperson.day09;

import com.vmperson.day06.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName: UserBehaviorAnalysisWithSQL
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/13  9:44
 * @Version: 1.0
 */
public class UserBehaviorAnalysisWithSQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        SingleOutputStreamOperator<UserBehavior> resource = env.readTextFile("D:\\CodeWorkSpace\\flink_code_java\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] split = value.split(",");

                        return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        tableEnv.createTemporaryView("t",
                resource,
                $("itemId"),
                $("timestamp").rowtime().as("ts")
        );

        Table table = tableEnv.sqlQuery("select * from ( " +
                " select *,ROW_NUMBER() OVER(PARTITION by windowEnd order By itemCount desc) as row_num from ( " +
                " select itemId,count(itemId) itemCount,HOP_END(ts,INTERVAL '5' MINUTE ,INTERVAL '1' hour) windowEnd from t group by itemId, HOP(ts,INTERVAL '5' MINUTE ,INTERVAL '1' hour)) " +
                " ) where row_num <= 3");
        tableEnv.toRetractStream(table, Row.class).print();

        env.execute(" 使用FlinkSQL的方式实现实时TOPN");

    }


}