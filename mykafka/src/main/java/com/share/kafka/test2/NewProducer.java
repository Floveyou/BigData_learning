package com.share.kafka.test2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @user: share
 * @date: 2019/2/18
 * @description:
 */
public class NewProducer {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "s102:9092,s103:9092,s104:9092");
        props.put("acks", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class","com.share.kafka.test2.NewPartition");

        long start = System.currentTimeMillis();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("t10", Integer.toString(i), Integer.toString(i));
            //同步状态
            Future<RecordMetadata> send = producer.send(record);
            System.out.println(send.get().toString());
        }
        System.out.println(System.currentTimeMillis() - start);
        producer.close();
    }
}
