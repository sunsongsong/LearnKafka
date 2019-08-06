package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @ClassName: PartitionerDemo
 * @Description: 自定义分区demo
 * @Author sunsongsong
 * @Date 2019/7/31 18:58
 * @Version 1.0
 */
public class PartitionerDemo implements Partitioner {


    @Override
    public int partition(String s, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        if (Integer.parseInt((String)key)%3==1){
            return 0;
        }else if (Integer.parseInt((String)key)%3==2){
            return 1;
        }else{
            return 2;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
