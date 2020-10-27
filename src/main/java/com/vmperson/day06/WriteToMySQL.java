package com.vmperson.day06;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: WriteToMySQL
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/10  9:23
 * @Version: 1.0
 */
public class WriteToMySQL {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream.addSink(new MySQLSink());


        env.execute("测试将数据输出到Mysql!");

    }


    public static class MySQLSink extends RichSinkFunction<SensorReading> {

        private Connection conn;
        private PreparedStatement updateStatement;
        private PreparedStatement insertStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/Spark?characterEncoding=utf8&useSSL=false",
                    "root",
                    "123456"
            );
            insertStatement = conn.prepareStatement("insert into sensor values (?,?)");
            updateStatement = conn.prepareStatement("update  sensor set  temp= ? where id= ? ");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStatement.setDouble(1, value.curFTemp);
            updateStatement.setString(2, value.sensorId);
            updateStatement.execute();

            if (updateStatement.getUpdateCount() == 0) {
                insertStatement.setString(1, value.sensorId);
                insertStatement.setDouble(2, value.curFTemp);
                insertStatement.execute();
            }


        }

        @Override
        public void close() throws Exception {
            super.close();
            insertStatement.close();
            updateStatement.close();
            conn.close();
        }
    }


}