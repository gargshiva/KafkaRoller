package com.gargshiva.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.gargshiva.tutorial1.constants.KafkaConstants.brokerList;
import static com.gargshiva.tutorial1.constants.KafkaConstants.topicName;

/**
 * Assign and Seek is basically used in the Replay
 */
public class KafkaConsumerAssignAndSeek {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerAssignAndSeek.class);

    public static void main(String[] args) {
        // Define Consumer Props
        Properties properties = new Properties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        // Create Kafka Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // Assign (Topic + Partition to Consumer)
        TopicPartition topicPartition = new TopicPartition(topicName, 1);
        kafkaConsumer.assign(Collections.singletonList(topicPartition));

        // Seek (Seek at particular offset)
        Long offsetToReadFrom = 1L;
        kafkaConsumer.seek(topicPartition,
                offsetToReadFrom);

        boolean keepOnReading = true;
        int messagesReadSoFar = 0;
        int messagesToRead = 10;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> rec : records) {
                ++messagesReadSoFar;
                log.info("Key : {} ,Value :{}", rec.key(), rec.value());
                if (messagesReadSoFar >= messagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        log.info("Application Exited");
    }
}
