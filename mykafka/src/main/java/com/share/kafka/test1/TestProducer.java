package com.share.kafka.test1;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

/**
 * @user: share
 * @date: 2019/2/17
 * @description: 测试通过API编写Kafka生产者
 */
public class TestProducer {
    public static void main(String[] args) {
        //构造java配置文件
        Properties prop = new Properties();
        prop.put("metadata.broker.list","s102:9092,s103:9092,s104:9092");
        prop.put("serializer.class","kafka.serializer.StringEncoder");
        prop.put("key.serializer.class","kafka.serializer.StringEncoder");
        prop.put("producer.type","async");
        prop.put("request.required.acks","0");
        //定义分区函数
        prop.put("partitioner.class","com.share.kafka.test1.MyPartition");
        //prop.put("batch.num.messages","1000");;
        //prop.put("queue.buffer.max.ms","5000");

        //通过java配置文件初始化producer配置文件
        ProducerConfig config = new ProducerConfig(prop);

        //通过producer配置文件，初始化producer
        Producer<String, String> producer = new Producer<String, String>(config);

        String[] arr = {"a","b","c","d"};
        Random r = new Random();

        long start = System.currentTimeMillis();
        //构造message数据(包括k-v)KeyedMessage<topic,message>
        for (int i = 1; i < 100000; i++) {
            KeyedMessage<String, String> msg = new KeyedMessage<String, String>("t1",  arr[r.nextInt(4)],"tom"+i);
            producer.send(msg);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(System.currentTimeMillis() - start);

        producer.close();

    }
}
