package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class DConsumer {

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "node:9092");
        prop.put("group.id", "test8");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //如果是之前存在的group.id
        KafkaConsumer consumer = new KafkaConsumer(prop);
        TopicPartition p = new TopicPartition("test2", 2);
        //指定消·费topic的那个分区
        consumer.assign(Arrays.asList(p));
        //指定从topic的分区的某个offset开始消费
        //consumer.seekToBeginning(Arrays.asList(p));
        consumer.seek(p, 5);
        //consumer.subscribe(Arrays.asList("test2"));

        //如果是之前不存在的group.id
        //Map<TopicPartition, OffsetAndMetadata> hashMaps = new HashMap<TopicPartition, OffsetAndMetadata>();
        //hashMaps.put(new TopicPartition("test2", 0), new OffsetAndMetadata(0));
        //consumer.commitSync(hashMaps);
        //consumer.subscribe(Arrays.asList("test2"));
        while (true) {
            ConsumerRecords<String, String> c = consumer.poll(100);
            for (ConsumerRecord<String, String> c1 : c) {
                System.out.println("Key: " + c1.key() + " Value: " + c1.value() + " Offset: " + c1.offset() + " Partitions: " + c1.partition());

            }
        }
    }


}
