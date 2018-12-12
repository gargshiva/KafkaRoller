package com.gargshiva.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.gargshiva.tutorial1.constants.KafkaConstants.*;

/**
 * Define Consumer Props
 * Create Kafka Consumer
 * Subscribe to topic
 * Consume message
 */
public class KafkaConsumerV1 {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerV1.class);

    public static void main(String[] args) {

        // Define Consumer Props
        Properties props = new Properties();
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Kafka Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // Subscribe to topics
        kafkaConsumer.subscribe(Collections.singleton(topicName));

        // Read Messages
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
            records.forEach((record) -> {
                LOGGER.info("Key : {} , Value : {}", record.key(), record.value());
            });
        }


    }
}
