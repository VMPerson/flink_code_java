package com.vmperson.day08;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @ClassName: AggregateFunctionExample
 * @Description: TODO 创建自定义聚合函数
 * @Author: VmPerson
 * @Date: 2020/10/12  19:42
 * @Version: 1.0
 */
public class AggregateFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> streamSource = env.addSource(new SensorSource());


        //使用FlinkSQl的方式
        tableEnvironment.createTemporaryView("sensor", streamSource);
        //注册聚合函数
        tableEnvironment.registerFunction("avgTemp", new AvgTemp());
        Table table = tableEnvironment.sqlQuery("select sensorId,avgTemp(curFTemp) from sensor group by sensorId");
        tableEnvironment.toRetractStream(table, Row.class).print();


        env.execute("自定义聚合函数！");


    }


    public static class AvgTempAcc {
        public Double sum = 0.0;
        public Integer count = 0;

        public AvgTempAcc() {
        }

        public AvgTempAcc(Double sum, Integer count) {
            this.sum = sum;
            this.count = count;
        }
    }

    public static class AvgTemp extends AggregateFunction<Double, AvgTempAcc> {

        @Override
        public Double getValue(AvgTempAcc avgTempAcc) {
            return avgTempAcc.sum / avgTempAcc.count;
        }

        @Override
        public AvgTempAcc createAccumulator() {
            return new AvgTempAcc();
        }

        public void accumulate(AvgTempAcc acc, Double temp) {
            acc.sum += temp;
            acc.count += 1;
        }


    }

}