package com.vmperson.day08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName: TableFunctionExample
 * @Description: TODO 用户自定义表函数
 * @Author: VmPerson
 * @Date: 2020/10/12  18:50
 * @Version: 1.0
 */
public class TableFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        //添加数据源
        DataStreamSource<String> stream = env.fromElements("hello#world", "bigdata#atguigu");

        tableEnvironment.createTemporaryView("tmp", stream, $("s"));

        //tableAPI方式
     /*   Table select = tableEnvironment
                .from("tmp")
                .joinLateral(call(SplitFunction.class, $("s")))
                .select($("s"), $("word"), $("length"));
        tableEnvironment.toAppendStream(select, Row.class).print();*/


        //FlinkSQL的方式

        //注册UDF函数
        tableEnvironment.createTemporarySystemFunction("splitFunction", SplitFunction.class);
        Table table = tableEnvironment.sqlQuery("SELECT s, word, length FROM tmp, LATERAL TABLE(SplitFunction(s))");
        tableEnvironment.toAppendStream(table, Row.class).print();


        env.execute("自定义表函数案例！");


    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {
        public void eval(String str) {
            for (String s : str.split("#")) {
                collect(Row.of(s, s.length()));
            }
        }
    }


}