package com.harold;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList("demo-topic"));

        System.out.println("Consumidor Kafka iniciado. Esperando mensajes...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("-----------------------------------------");
                    System.out.println("Mensaje recibido:");
                    System.out.println("Topic: " + record.topic());
                    System.out.println("Partición: " + record.partition());
                    System.out.println("Offset: " + record.offset());
                    System.out.println("Timestamp: " + record.timestamp());
                    System.out.println("Valor: " + record.value());
                }
            }
        } catch (Exception e) {
            System.out.println("Error en el consumidor: " + e.getMessage());
        } finally {
            consumer.close();
            System.out.println("Consumidor finalizado.");
        }
    }
}
