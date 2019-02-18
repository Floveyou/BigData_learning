package com.share.kafka.test2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @user: share
 * @date: 2019/2/18
 * @description:Consumer事务
 */
public class ConsumerTX {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","s102:9092,s103:9092,s104:9092");
        props.put("group.id", "g3");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "100");
        // props.put("isolation.level","read_uncommitted");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //通过subscribe方法，指定多主题消费
        consumer.subscribe(Arrays.asList("tx"));


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5000);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
