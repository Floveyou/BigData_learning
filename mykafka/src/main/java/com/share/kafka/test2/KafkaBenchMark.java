package com.share.kafka.test2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;

/**
 * @user: share
 * @date: 2019/2/18
 * @description:Kafka吞吐量测试
 */
public class KafkaBenchMark {
    //单个生产者线程，没有复制
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "s102:9092,s103:9092,s104:9092");
        props.put("acks", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        long start = System.currentTimeMillis();
        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 1024 * 1024; i++) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("t11", new byte[1024]);
            //异步状态
            producer.send(record);
        }
        System.out.println(System.currentTimeMillis() - start);
        producer.close();
    }


    @Test
    //单生产者线程，3x异步复制
    public void test2() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "s102:9092,s103:9092,s104:9092");
        props.put("acks", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        long start = System.currentTimeMillis();
        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 1024 * 1024; i++) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("t12", new byte[1024]);
            //异步状态
            producer.send(record);
        }
        System.out.println(System.currentTimeMillis() - start);
        producer.close();
    }

    @Test
    //单生产者线程，3x同步复制
    public void test3() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "s102:9092,s103:9092,s104:9092");
        props.put("acks", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        long start = System.currentTimeMillis();
        Producer<String, byte[]> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 1024 * 1024; i++) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("t13", new byte[1024]);
            //异步状态
            producer.send(record).get();
        }
        System.out.println(System.currentTimeMillis() - start);
        producer.close();
    }

    @Test
    //三个生产者线程，3x异步复制
    public void test4() {
        Thread t1 = new Thread() {
            @Override
            public void run() {
                try {
                    Properties props = new Properties();
                    props.put("bootstrap.servers", "s102:9092,s103:9092,s104:9092");
                    props.put("acks", "0");
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

                    long start = System.currentTimeMillis();
                    Producer<String, byte[]> producer = new KafkaProducer<>(props);
                    for (int i = 0; i < 1024 * 342; i++) {
                        ProducerRecord<String, byte[]> record = new ProducerRecord<>("t14", new byte[1024]);
                        //异步状态
                        producer.send(record);
                    }
                    System.out.println(System.currentTimeMillis() - start);
                    producer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        Thread t2 = new Thread() {
            @Override
            public void run() {
                try {
                    Properties props = new Properties();
                    props.put("bootstrap.servers", "s102:9092,s103:9092,s104:9092");
                    props.put("acks", "0");
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

                    long start = System.currentTimeMillis();
                    Producer<String, byte[]> producer = new KafkaProducer<>(props);
                    for (int i = 0; i < 1024 * 341; i++) {
                        ProducerRecord<String, byte[]> record = new ProducerRecord<>("t14", new byte[1024]);
                        //异步状态
                        producer.send(record);
                    }
                    System.out.println(System.currentTimeMillis() - start);
                    producer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        Thread t3 = new Thread() {
            @Override
            public void run() {
                try {
                    Properties props = new Properties();
                    props.put("bootstrap.servers", "s102:9092,s103:9092,s104:9092");
                    props.put("acks", "0");
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

                    long start = System.currentTimeMillis();
                    Producer<String, byte[]> producer = new KafkaProducer<>(props);
                    for (int i = 0; i < 1024 * 341; i++) {
                        ProducerRecord<String, byte[]> record = new ProducerRecord<>("t14", new byte[1024]);
                        //异步状态
                        producer.send(record);
                    }
                    System.out.println(System.currentTimeMillis() - start);
                    producer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        t1.start();
        t2.start();
        t3.start();
        while (true) {

        }
    }
}
