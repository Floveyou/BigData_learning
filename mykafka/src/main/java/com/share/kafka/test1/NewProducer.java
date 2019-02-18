package com.share.kafka.test1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @user: share
 * @date: 2019/2/18
 * @description:测试通过新API编写Kafka生产者
 */
public class NewProducer {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "s102:9092,s103:9092,s104:9092");
        props.put("acks", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class","com.share.kafka.test1.NewPartition");

        long start = System.currentTimeMillis();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("t1", Integer.toString(i), Integer.toString(i));
            System.out.println("key:" + i + "\t" + Math.abs(Integer.toString(i).hashCode()));
            System.out.println("===========================================");
            Thread.sleep(500);
            producer.send(record);
        }
        System.out.println(System.currentTimeMillis() - start);
        producer.close();
    }
}
