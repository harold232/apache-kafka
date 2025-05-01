package com.harold;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaConsumerApp {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Ingresa los topics de los que quieres consumir (separados por coma): ");
        String[] topicArray = scanner.nextLine().split(",");
        List<String> topics = Arrays.stream(topicArray)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        if (topics.isEmpty()) {
            System.out.println("No se especificaron topics. Terminando.");
            return;
        }

        System.out.print("¿Quieres reenviar los mensajes a otro topic? (s/n): ");
        boolean reenviar = scanner.nextLine().trim().equalsIgnoreCase("s");

        String outputTopic = null;
        KafkaProducer<String, String> producer = null;

        if (reenviar) {
            System.out.print("Ingresa el nombre del topic de destino: ");
            outputTopic = scanner.nextLine();

            Properties producerProps = new Properties();
            producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            producer = new KafkaProducer<>(producerProps);
        }

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-multitopic");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(topics);

        System.out.println("Consumidor iniciado. Subscrito a: " + String.join(", ", topics));
        if (reenviar) {
            System.out.println("Reenviando mensajes a: " + outputTopic);
        }

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("---------------------------------------------------");
                    System.out.println("Topic: " + record.topic());
                    System.out.println("Partición: " + record.partition());
                    System.out.println("Offset: " + record.offset());
                    System.out.println("Valor: " + record.value());

                    if (reenviar && producer != null && outputTopic != null) {
                        ProducerRecord<String, String> forward = new ProducerRecord<>(outputTopic, record.value());
                        producer.send(forward);
                        System.out.println("→ Reenviado a: " + outputTopic);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error en el consumidor: " + e.getMessage());
        } finally {
            consumer.close();
            if (producer != null) {
                producer.close();
            }
            System.out.println("Consumidor finalizado.");
        }
    }
}
