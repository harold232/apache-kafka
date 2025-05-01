package com.harold;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerApp {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Ingresa el nombre del topic al que quieres enviar mensajes: ");
        String topic = scanner.nextLine();

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        System.out.println("Productor Kafka iniciado. Escribe mensajes (escribe 'salir' para terminar):");

        while (true) {
            System.out.print("Mensaje: ");
            String input = scanner.nextLine();
            if ("salir".equalsIgnoreCase(input)) break;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, input);

            producer.send(record, (metadata, e) -> {
                if (e == null) {
                    System.out.println("Mensaje enviado a " + metadata.topic() + " [Partición " + metadata.partition() + "] Offset: " + metadata.offset());
                } else {
                    System.err.println("Error al enviar mensaje: " + e.getMessage());
                }
            });
        }

        producer.close();
        scanner.close();
        System.out.println("Productor finalizado.");
    }
}
