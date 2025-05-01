package com.harold.prueba;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsAdvancedDemo {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-advanced-demo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Habilitar procesamiento exactamente una vez
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Crear topología de streams
        final StreamsBuilder builder = new StreamsBuilder();

        // ==================== EJEMPLO 1: FILTRADO Y TRANSFORMACIÓN BÁSICA ====================
        // Consumir datos del topic de ventas
        KStream<String, String> ventasStream = builder.stream("ventas-topic");

        // Filtrar ventas de más de $1000 y enviarlas a un tema específico
        ventasStream
                .filter((key, value) -> {
                    try {
                        // Asumimos que el valor es un JSON: {"producto": "X", "monto": 1234}
                        return value.contains("\"monto\":") &&
                                Double.parseDouble(value.split("\"monto\":")[1].split("[,}]")[0].trim()) > 1000;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .to("ventas-grandes-topic");

        // ==================== EJEMPLO 2: AGREGACIÓN CON VENTANAS TEMPORALES ====================
        // Stream para analizar clics web
        KStream<String, String> webClicksStream = builder.stream("web-clicks-topic");

        // Contar clics por página en ventanas de 1 minuto
        webClicksStream
                .groupBy((key, value) -> {
                    // Asumimos valor: {"pagina": "/home", "usuario": "123"}
                    try {
                        return value.split("\"pagina\":\"")[1].split("\"")[0];
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count()
                .toStream()
                .map((windowedKey, count) -> {
                    String pagina = windowedKey.key();
                    String ventana = windowedKey.window().startTime() + " - " + windowedKey.window().endTime();
                    return KeyValue.pair(pagina,
                            String.format("{\"pagina\": \"%s\", \"periodo\": \"%s\", \"conteo\": %d}",
                                    pagina, ventana, count));
                })
                .to("paginas-stats-topic");

        // ==================== EJEMPLO 3: JOIN ENTRE STREAMS ====================
        // Stream de usuarios
        KStream<String, String> usuariosStream = builder.stream("usuarios-topic");

        // Stream de compras
        KStream<String, String> comprasStream = builder.stream("compras-topic");

        // Join de usuarios y compras usando el ID de usuario como clave común
        // La ventana de JOIN es de 5 minutos
        KStream<String, String> usuariosComprasJoin = usuariosStream.join(
                comprasStream,
                // Función de JOIN que combina los datos de usuario y compra
                (usuarioInfo, compraInfo) -> {
                    String nombreUsuario = usuarioInfo.contains("\"nombre\":") ?
                            usuarioInfo.split("\"nombre\":\"")[1].split("\"")[0] : "unknown";

                    String productoComprado = compraInfo.contains("\"producto\":") ?
                            compraInfo.split("\"producto\":\"")[1].split("\"")[0] : "unknown";

                    double montoCompra = compraInfo.contains("\"monto\":") ?
                            Double.parseDouble(compraInfo.split("\"monto\":")[1].split("[,}]")[0].trim()) : 0.0;

                    return String.format(
                            "{\"usuario\": \"%s\", \"producto\": \"%s\", \"monto\": %.2f, \"timestamp\": %d}",
                            nombreUsuario, productoComprado, montoCompra, System.currentTimeMillis()
                    );
                },
                // Ventana de tiempo para el JOIN
                JoinWindows.of(Duration.ofMinutes(5))
        );

        // Enviar resultados del JOIN a un nuevo topic
        usuariosComprasJoin.to("usuarios-compras-enriquecido-topic");

        // ==================== EJEMPLO 4: AGREGACIÓN AVANZADA CON GROUPBY ====================
        // Calcular total de ventas por categoría
        KStream<String, String> productosStream = builder.stream("productos-topic");

        productosStream
                .groupBy((key, value) -> {
                    // Extraer la categoría del producto
                    try {
                        return value.split("\"categoria\":\"")[1].split("\"")[0];
                    } catch (Exception e) {
                        return "sin-categoria";
                    }
                })
                .aggregate(
                        // Inicializador
                        () -> "0.0",
                        // Agregador
                        (key, value, aggregate) -> {
                            double currentTotal = Double.parseDouble(aggregate);
                            double precio = 0.0;

                            try {
                                precio = Double.parseDouble(value.split("\"precio\":")[1].split("[,}]")[0].trim());
                            } catch (Exception e) {
                                // Error al parsear, mantenemos el valor actual
                            }

                            return String.valueOf(currentTotal + precio);
                        },
                        // Se especifica el almacén de estado
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .map((categoria, total) -> {
                    return KeyValue.pair(categoria,
                            String.format("{\"categoria\": \"%s\", \"total_ventas\": %s}",
                                    categoria, total));
                })
                .to("ventas-por-categoria-topic");

        // ==================== EJEMPLO 5: PROCESAMIENTO DE MÚLTIPLES STREAMS CON BRANCH ====================
        // Dividir un stream en múltiples streams según condiciones
        KStream<String, String> transaccionesStream = builder.stream("transacciones-topic");

        // Separar transacciones en tres categorías usando branch
        KStream<String, String>[] branches = transaccionesStream.branch(
                // Primera condición: transacciones de alto valor (>$10,000)
                (key, value) -> {
                    try {
                        return value.contains("\"monto\":") &&
                                Double.parseDouble(value.split("\"monto\":")[1].split("[,}]")[0].trim()) > 10000;
                    } catch (Exception e) {
                        return false;
                    }
                },
                // Segunda condición: transacciones de valor medio ($1,000-$10,000)
                (key, value) -> {
                    try {
                        double amount = Double.parseDouble(value.split("\"monto\":")[1].split("[,}]")[0].trim());
                        return amount >= 1000 && amount <= 10000;
                    } catch (Exception e) {
                        return false;
                    }
                },
                // Tercera condición: el resto (transacciones de bajo valor <$1,000)
                (key, value) -> true
        );

        // Procesar cada rama diferentemente
        branches[0].to("transacciones-alto-valor-topic");
        branches[1].to("transacciones-valor-medio-topic");
        branches[2].to("transacciones-bajo-valor-topic");

        // ==================== CONSTRUIR Y EJECUTAR LA TOPOLOGÍA ====================
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, config);

        // Imprimir la topología para depuración
        System.out.println(topology.describe());

        final CountDownLatch latch = new CountDownLatch(1);

        // Limpieza adecuada al cerrar
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
                System.out.println("Streams cerrado correctamente");
            }
        });

        try {
            streams.start();
            System.out.println("Kafka Streams avanzado iniciado");
            latch.await();
        } catch (Throwable e) {
            System.err.println("Error al iniciar Kafka Streams: " + e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}
