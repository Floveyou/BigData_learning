package com.share.kafka.test2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @user: share
 * @date: 2019/2/18
 * @description:
 */
public class NewConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","s102:9092,s103:9092,s104:9092");
        props.put("group.id", "g3");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "100");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //通过subscribe方法，指定多主题消费
        //consumer.subscribe(Arrays.asList("t4","t5"));

        //指定分区消费
//        ArrayList<TopicPartition> list = new ArrayList<TopicPartition>();
//        TopicPartition tp = new TopicPartition("t1", 0);
//        TopicPartition tp2 = new TopicPartition("t4", 0);
//        TopicPartition tp3 = new TopicPartition("t4", 1);
//        TopicPartition tp4 = new TopicPartition("t4", 2);
//        list.add(tp);
//        list.add(tp2);
//        list.add(tp3);
//        list.add(tp4);
//        consumer.assign(list);


        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<TopicPartition, OffsetAndMetadata>();
        //指定分区
        TopicPartition tp = new TopicPartition("t4", 0);
        //指定偏移量
        OffsetAndMetadata metadata = new OffsetAndMetadata(3);
        offset.put(tp,metadata);
        //修改偏移量
        consumer.commitSync(offset);
        //订阅主题
        //consumer.subscribe(Arrays.asList("t4"));
        consumer.assign(Arrays.asList(tp));

        //指定分区
//        TopicPartition tp = new TopicPartition("t4", 0);
//        consumer.assign(Arrays.asList(tp));
//        //修改搜索指针
//        consumer.seek(tp,10);


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5000);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            consumer.commitSync();
        }
    }
}
