package com.share.kafka.test1;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @user: share
 * @date: 2019/2/18
 * @description: 测试新API的分区
 */
public class NewPartition implements Partitioner {

    //对于新分区函数，可以指定value进行分区
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取分区个数
        List<PartitionInfo> infos = cluster.partitionsForTopic(topic);
        int numPartition = infos.size();

        //将key的hashcode取正值，和分区数取余
        return Math.abs(key.hashCode()) % numPartition;

    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}

