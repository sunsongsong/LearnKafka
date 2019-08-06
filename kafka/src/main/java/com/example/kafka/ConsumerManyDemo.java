package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName: ConsumerManyDemo
 * @Description: 多个消费组测试
 * @Author sunsongsong
 * @Date 2019/7/31 16:19
 * @Version 1.0
 */
public class ConsumerManyDemo {

    public static Properties getProperties(String brokers, String groupId){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    public static void main(String[] args) {
        String brokers = "node01:9092,node02:9092,node03:9092";
        List<String> topicList = new ArrayList();
        topicList.add("test");

        //设置为同一消费组
        Properties props0 = getProperties(brokers, "aaa");
        Properties props1 = getProperties(brokers, "aaa");
//        Properties props2 = getProperties(brokers, "aaa");
        KafkaConsumer<String, String> consumer0 = new KafkaConsumer<String,String>(props0);
        consumer0.subscribe(topicList);
        KafkaConsumer<String, String> consumer1 = new KafkaConsumer<String,String>(props1);
        consumer1.subscribe(topicList);
//        KafkaConsumer<String, String> consumer2 = new KafkaConsumer<String,String>(props2);
//        consumer2.subscribe(topicList);
        //消费者订阅我们的topic

        new ConsumerThread("0",consumer0).start();
        new ConsumerThread("1",consumer1).start();
//        new ConsumerThread("2",consumer2).start();

        //测试结果：说明一个partitioner只能被一个消费组下的一个线程消费
        //如果线程数 > partitioner数，多余的线程也没法消费
        //如果线程数 < partitioner数，一个线程消费多个partitioner
        //实际设置：线程数可以比partitioner数多1,2个，这样如果其中的一个线程挂掉,空闲的线程就会定上
    }

}

class ConsumerThread extends Thread{

    private String threadName;

    private KafkaConsumer<String, String> consumer;

    public ConsumerThread(String threadName, KafkaConsumer<String, String> consumer) {
        this.threadName = threadName;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for(ConsumerRecord<String, String> record : consumerRecords){
                System.out.printf("当前执行线程：%s, partition = %d,offset = %d, key = %s, value = %s%n",
                        threadName, record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}
