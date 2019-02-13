package com.kafka;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.utils.Json;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.common.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaProducer {
    public static void main(String a[]) throws IOException {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.Producer producer = new org.apache.kafka.clients.producer.KafkaProducer(configProperties);


        //TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
        String topicName="sample.request";
        String payload= readJsonPayload();
        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, payload);
        producer.send(rec);

        producer.close();
    }

    private static String readJsonPayload() throws IOException {
        return FileUtils.readFileToString(Paths.get("D:\\workspace\\poc\\KafkaSample1\\src\\main\\resources\\sampleRequest.json").toFile(),Charset.defaultCharset());
    }
}
