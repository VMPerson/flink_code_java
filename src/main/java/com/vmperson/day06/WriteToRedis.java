package com.vmperson.day06;

import com.vmperson.day02.util.SensorReading;
import com.vmperson.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @ClassName: WriteToRedis
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/9  20:52
 * @Version: 1.0
 */
public class WriteToRedis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();
        stream.addSink(new RedisSink<SensorReading>(jedisPoolConfig, new MyRedisSink()));

        env.execute("将结果输出到redis");

    }

    public static class MyRedisSink implements RedisMapper<SensorReading> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getSensorId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.getCurFTemp() + "";
        }
    }

}