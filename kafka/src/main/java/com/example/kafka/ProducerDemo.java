package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName: ProducerDemo
 * @Description: kafka生产者案例
 * @Author sunsongsong
 * @Date 2019/7/31 11:30
 * @Version 1.0
 */
public class ProducerDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //自定义分区需要添加上我们自定义分区的那个类
//        props.put("partitioner.class", "cn.itcast.kafka.kafkaPartitioner.MyOwnPartitioner");

        Producer<String, String> producer = new KafkaProducer<String,String>(props);
        for (int i = 100; i < 130; i++){
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();

    }



}
