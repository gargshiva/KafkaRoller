package com.gargshiva.tutorial1.producer;


import com.gargshiva.tutorial1.constants.KafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.gargshiva.tutorial1.constants.KafkaConstants.topicName;

/**
 * Define Producer Properties
 * Create Kafka Producer
 * Create Message to be sent
 * Send Messages
 * kafka-console-producer.sh --topic <topic-name> --broker-list <brokers>
 */
public class KafkaProducerV1 {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaProducerV1.class);

    public static void main(String[] args) throws Exception {
        // Define Props
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.brokerList);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps);

        // Create messages to be sent
        // Key is used to decide the partition. Message with same key , goes to same partition.
        ProducerRecord<String, String> message = new ProducerRecord<>(topicName, "Hello", "Greeting");

        for (int i = 0; i <= 9; ++i) {
            // Send Messages - async
            kafkaProducer.send(message, (recordMetadata, exception) -> {
                if (exception == null) {
                    LOGGER.info("Message Sent to Topic: {} , Partition: {} ,Offset:{}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                } else {
                    LOGGER.error("Exception while Producing message : {} ", exception.getMessage());
                }
            });
        }


        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
