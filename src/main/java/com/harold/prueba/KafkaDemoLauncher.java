package com.harold.prueba;

import com.harold.KafkaConsumerDemo;
import com.harold.KafkaProducerDemo;
import com.harold.KafkaStreamsDemo;

import java.util.Scanner;

public class KafkaDemoLauncher {

    public static void main(String[] args) {
        System.out.println("=== Demo de Apache Kafka ===");
        System.out.println("Selecciona un componente para ejecutar:");
        System.out.println("1. Productor básico (enviar mensajes)");
        System.out.println("2. Consumidor básico (recibir mensajes)");
        System.out.println("3. Streams básico (transformación simple)");
        System.out.println("4. Generador de datos avanzados");
        System.out.println("5. Streams avanzado (procesamiento complejo)");
        System.out.println("6. Crear topics necesarios");
        System.out.println("0. Salir");

        Scanner scanner = new Scanner(System.in);
        System.out.print("\nSelección: ");
        int option = scanner.nextInt();

        try {
            switch (option) {
                case 1:
                    System.out.println("Iniciando productor básico...");
                    KafkaProducerDemo.main(null);
                    break;
                case 2:
                    System.out.println("Iniciando consumidor básico...");
                    KafkaConsumerDemo.main(null);
                    break;
                case 3:
                    System.out.println("Iniciando Streams básico...");
                    KafkaStreamsDemo.main(null);
                    break;
                case 4:
                    System.out.println("Iniciando generador de datos avanzados...");
                    KafkaDataGenerator.main(null);
                    break;
                case 5:
                    System.out.println("Iniciando Streams avanzado...");
                    KafkaStreamsAdvancedDemo.main(null);
                    break;
                case 6:
                    System.out.println("Creando topics necesarios...");
                    createTopics();
                    break;
                case 0:
                    System.out.println("Saliendo...");
                    break;
                default:
                    System.out.println("Opción no válida");
            }
        } catch (Exception e) {
            System.err.println("Error al ejecutar el componente: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void createTopics() {
        // Implementación para crear los topics necesarios
        String[] command = {
                "/bin/bash",
                "-c",
                "cd kafka_2.13-3.5.1 && " +
                        "bin/kafka-topics.sh --create --topic demo-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic demo-topic-processed --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic ventas-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic ventas-grandes-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic web-clicks-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic paginas-stats-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic usuarios-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic compras-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic usuarios-compras-enriquecido-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic productos-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic ventas-por-categoria-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic transacciones-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic transacciones-alto-valor-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic transacciones-valor-medio-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists && " +
                        "bin/kafka-topics.sh --create --topic transacciones-bajo-valor-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists"
        };

        try {
            Process process = Runtime.getRuntime().exec(command);
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Topics creados correctamente");
            } else {
                System.err.println("Error al crear topics. Código de salida: " + exitCode);
            }
        } catch (Exception e) {
            System.err.println("Error al ejecutar el comando: " + e.getMessage());
        }
    }
}