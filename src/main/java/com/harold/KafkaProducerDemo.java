package com.harold;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Scanner scanner = new Scanner(System.in);
        String topic = "demo-topic";

        System.out.println("Productor Kafka iniciado. Escribe mensajes (escribe 'salir' para terminar):");

        String line;
        while (!(line = scanner.nextLine()).equals("salir")) {

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        System.out.println("Mensaje enviado con éxito a: " +
                                metadata.topic() + " | Partición: " +
                                metadata.partition() + " | Offset: " +
                                metadata.offset() + " | Timestamp: " +
                                metadata.timestamp());
                    } else {
                        System.out.println("Error al enviar mensaje: " + e.getMessage());
                    }
                }
            });
        }

        producer.close();
        scanner.close();
        System.out.println("Productor finalizado.");
    }
}