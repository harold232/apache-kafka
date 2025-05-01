package com.harold;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsApp {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Ingresa el topic de entrada (lectura): ");
        String inputTopic = scanner.nextLine().trim();

        System.out.print("Ingresa el topic de salida (escritura): ");
        String outputTopic = scanner.nextLine().trim();

        if (inputTopic.isEmpty() || outputTopic.isEmpty()) {
            System.out.println("Topics inválidos. Terminando.");
            return;
        }

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app-" + inputTopic + "-" + outputTopic);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream(inputTopic);

        KStream<String, String> processedStream = sourceStream.mapValues(value -> {
            System.out.println("Procesando mensaje: " + value);
            return value.toUpperCase();
        });

        processedStream.to(outputTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Apagando Kafka Streams...");
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            System.out.println("Kafka Streams iniciado.");
            System.out.println("Leyendo desde: " + inputTopic);
            System.out.println("Escribiendo hacia: " + outputTopic);
            latch.await();
        } catch (Throwable e) {
            System.err.println("Error en Kafka Streams: " + e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}
