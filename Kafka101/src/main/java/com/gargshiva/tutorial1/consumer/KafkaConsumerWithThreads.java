package com.gargshiva.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.gargshiva.tutorial1.constants.KafkaConstants.*;

public class KafkaConsumerWithThreads {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerWithThreads.class);

    public static void main(String[] args) {
        new KafkaConsumerWithThreads().run();
    }

    public void run() {
        // Define Consumer Props
        Properties props = new Properties();
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID + 1);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerRunnable = new ConsumerRunnable(latch, props, topicName);

        Thread th = new Thread(consumerRunnable);
        th.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException in) {
                LOGGER.error("Application is interrupted {}", in);
            }
            LOGGER.info("Application has exited !");
        }));

        try {
            latch.await();
        } catch (InterruptedException in) {
            LOGGER.error("Application is interrupted {}", in);
        } finally {
            LOGGER.info("Application is closing");
        }

    }


    public class ConsumerRunnable implements Runnable {
        private final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunnable.class);
        private KafkaConsumer<String, String> kafkaConsumer = null;
        private CountDownLatch latch;

        public ConsumerRunnable(CountDownLatch countDownLatch, Properties properties, String topicName) {
            this.latch = countDownLatch;
            kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Collections.singleton(topicName));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));
                    records.forEach((record) -> {
                        LOGGER.info("Key : {} , Value : {}", record.key(), record.value());
                    });
                }
            } catch (WakeupException wakeUpException) {
                LOGGER.info("Consumer has recieved shutdown; Shutting down the consumer");
            } finally {
                kafkaConsumer.close(Duration.ofMinutes(1));
                latch.countDown();
            }
        }

        public void shutdown() {
            // wakeUp() method is used to interuppt current consumer.
            kafkaConsumer.wakeup();
        }
    }
}

