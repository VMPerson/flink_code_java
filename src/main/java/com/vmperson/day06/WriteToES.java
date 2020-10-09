package com.vmperson.day06;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName: WriteToES
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/9  20:32
 * @Version: 1.0
 */
public class WriteToES {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        ArrayList<HttpHost> list = new ArrayList<>();
        list.add(new HttpHost("hadoop102", 9200, "http"));
        ElasticsearchSink.Builder<SensorReading> sensorReadingBuilder1 = new ElasticsearchSink.Builder<>(
                list,
                new ElasticsearchSinkFunction<SensorReading>() {
                    @Override
                    public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        HashMap<String, String> map = new HashMap<>();
                        map.put("data", sensorReading.toString());

                        IndexRequest source = Requests.indexRequest().index("sensor0421")
                                .type("sensor").source(map);
                        requestIndexer.add(source);

                    }
                }
        );
        sensorReadingBuilder1.setBulkFlushMaxActions(1);
        stream.addSink(sensorReadingBuilder1.build());
        env.execute();

    }


}