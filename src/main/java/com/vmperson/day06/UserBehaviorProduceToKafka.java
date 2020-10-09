package com.vmperson.day06;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

/**
 * @ClassName: UserBehaviorProduceToKafka
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/9  15:34
 * @Version: 1.0
 */
public class UserBehaviorProduceToKafka {
    public static void main(String[] args) throws Exception {
        writeToKafka("hotItems");
    }

    private static void writeToKafka(String topic) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        File file =
                new File("D:\\CodeWorkSpace\\flink_code_java\\src\\main\\resources\\UserBehavior.csv");
        Scanner sc = new Scanner(file);

        while (sc.hasNextLine()) {
            producer.send(new ProducerRecord<String, String>(topic, sc.nextLine()));
        }


    }


}