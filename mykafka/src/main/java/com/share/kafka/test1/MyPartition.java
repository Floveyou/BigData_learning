package com.share.kafka.test1;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * @user: share
 * @date: 2019/2/18
 * @description:自定义分区
 */
public class MyPartition implements Partitioner {

    //必填项，不然无法通过配置文件配置
    public MyPartition (VerifiableProperties props) {
    }

    public int partition(Object key, int numPartitions) {
        if(key.toString().startsWith("a")){
            return 0;
        }
        if(key.toString().startsWith("b")){
            return 1;
        }
        else {
            return 2;
        }
    }
}
