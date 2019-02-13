package com.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.*;

public class KafkaConsumer {
    public static void main(String[] args) {


        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        org.apache.kafka.clients.consumer.KafkaConsumer kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("sample.request");
        kafkaConsumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords<String,String> records = kafkaConsumer.poll(10);
                for (ConsumerRecord record : records) {
                    Object payload = record.value();

                    Map<String,String> myMap = new HashMap<String, String>();
                    ObjectMapper objectMapper = new ObjectMapper();
                    myMap = objectMapper.readValue(payload.toString(), HashMap.class);
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), payload));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

}
