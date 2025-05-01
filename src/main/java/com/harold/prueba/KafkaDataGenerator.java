package com.harold.prueba;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class KafkaDataGenerator {
    private static final Random random = new Random();
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        // Configurar el productor de Kafka
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer<>(properties);

        try {
            // Crear los topics necesarios (asegúrate de tener los topics creados o habilitar la autocreación)
            System.out.println("Iniciando generación de datos de muestra...");

            // Generar datos para cada topic
            for (int i = 0; i < 10; i++) {
                // Generar con un ligero retardo para simular datos en tiempo real
                Thread.sleep(500);

                // Generar datos de usuario (de vez en cuando)
                if (i % 5 == 0) {
                    generarDatosUsuario(i);
                }

                // Generar datos de ventas
                generarDatosVentas(i);

                // Generar datos de clics web
                generarDatosClicsWeb(i);

                // Generar datos de compras (relacionados con usuarios)
                if (i % 3 == 0) {
                    generarDatosCompras(i);
                }

                // Generar datos de productos
                generarDatosProductos(i);

                // Generar datos de transacciones
                generarDatosTransacciones(i);

                System.out.println("Batch de datos #" + (i+1) + " enviado");
            }

        } catch (Exception e) {
            System.err.println("Error durante la generación de datos: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Cerrar el productor de forma adecuada
            producer.close();
            System.out.println("Generación de datos completada");
        }
    }

    private static void enviarMensaje(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error al enviar mensaje a " + topic + ": " + exception.getMessage());
                }
            }).get(); // Usar get() para hacer síncrono para simplificar la demo
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error en envío de mensaje: " + e.getMessage());
        }
    }

    private static void generarDatosUsuario(int i) {
        String userId = "user" + (i % 10 + 1);
        String[] nombres = {"Ana", "Juan", "María", "Carlos", "Laura", "Miguel", "Sofía", "David", "Elena", "Pablo"};
        String[] ciudades = {"Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao", "Málaga", "Zaragoza", "Murcia"};

        String nombre = nombres[i % nombres.length];
        String ciudad = ciudades[random.nextInt(ciudades.length)];
        int edad = 20 + random.nextInt(50);

        String usuarioJson = String.format(
                "{\"id\": \"%s\", \"nombre\": \"%s\", \"ciudad\": \"%s\", \"edad\": %d, \"timestamp\": %d}",
                userId, nombre, ciudad, edad, System.currentTimeMillis()
        );

        enviarMensaje("usuarios-topic", userId, usuarioJson);
    }

    private static void generarDatosVentas(int i) {
        String[] productos = {"Laptop", "Smartphone", "Tablet", "Monitor", "Teclado", "Mouse", "Impresora", "Altavoces"};
        String productoId = "prod" + (random.nextInt(100) + 1);
        String producto = productos[random.nextInt(productos.length)];
        double monto = 100 + random.nextDouble() * 2000; // Entre $100 y $2100

        String ventaJson = String.format(
                "{\"id\": \"%s\", \"producto\": \"%s\", \"monto\": %.2f, \"timestamp\": %d}",
                productoId, producto, monto, System.currentTimeMillis()
        );

        enviarMensaje("ventas-topic", productoId, ventaJson);
    }

    private static void generarDatosClicsWeb(int i) {
        String[] paginas = {"/home", "/productos", "/categorias", "/ofertas", "/cuenta", "/carrito", "/checkout", "/ayuda"};
        String[] usuarios = {"user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"};

        String pagina = paginas[random.nextInt(paginas.length)];
        String usuario = usuarios[random.nextInt(usuarios.length)];

        String clicJson = String.format(
                "{\"pagina\": \"%s\", \"usuario\": \"%s\", \"timestamp\": %d}",
                pagina, usuario, System.currentTimeMillis()
        );

        // Usar la página como clave para facilitar el agrupamiento
        enviarMensaje("web-clicks-topic", pagina, clicJson);
    }

    private static void generarDatosCompras(int i) {
        String userId = "user" + (i % 10 + 1); // Relacionado con los usuarios generados
        String[] productos = {"Laptop", "Smartphone", "Tablet", "Monitor", "Teclado", "Mouse", "Impresora", "Altavoces"};

        String producto = productos[random.nextInt(productos.length)];
        double monto = 50 + random.nextDouble() * 1500; // Entre $50 y $1550

        String compraJson = String.format(
                "{\"usuario_id\": \"%s\", \"producto\": \"%s\", \"monto\": %.2f, \"timestamp\": %d}",
                userId, producto, monto, System.currentTimeMillis()
        );

        // Usar userId como clave para facilitar el join
        enviarMensaje("compras-topic", userId, compraJson);
    }

    private static void generarDatosProductos(int i) {
        String[] productos = {"Laptop", "Smartphone", "Tablet", "Monitor", "Teclado", "Mouse", "Impresora", "Altavoces"};
        String[] categorias = {"Electrónica", "Informática", "Accesorios", "Periféricos", "Audio"};

        String productoId = "prod" + (random.nextInt(20) + 1);
        String nombre = productos[random.nextInt(productos.length)] + " " + (char)('A' + random.nextInt(5));
        String categoria = categorias[random.nextInt(categorias.length)];
        double precio = 50 + random.nextDouble() * 950; // Entre $50 y $1000

        String productoJson = String.format(
                "{\"id\": \"%s\", \"nombre\": \"%s\", \"categoria\": \"%s\", \"precio\": %.2f, \"timestamp\": %d}",
                productoId, nombre, categoria, precio, System.currentTimeMillis()
        );

        // Usar categoría como clave para facilitar la agregación
        enviarMensaje("productos-topic", categoria, productoJson);
    }

    private static void generarDatosTransacciones(int i) {
        String transaccionId = "trans" + (i * 10 + random.nextInt(10));
        String[] tipos = {"compra", "devolucion", "transferencia", "suscripcion", "pago"};

        String tipo = tipos[random.nextInt(tipos.length)];

        // Generar montos de diferentes rangos para probar branch
        double monto;
        int rangoSeleccionado = random.nextInt(10);

        if (rangoSeleccionado < 6) {
            // 60% transacciones de bajo valor
            monto = 10 + random.nextDouble() * 990; // $10-$1000
        } else if (rangoSeleccionado < 9) {
            // 30% transacciones de valor medio
            monto = 1000 + random.nextDouble() * 9000; // $1000-$10000
        } else {
            // 10% transacciones de alto valor
            monto = 10000 + random.nextDouble() * 90000; // $10000-$100000
        }

        String transaccionJson = String.format(
                "{\"id\": \"%s\", \"tipo\": \"%s\", \"monto\": %.2f, \"timestamp\": %d}",
                transaccionId, tipo, monto, System.currentTimeMillis()
        );

        enviarMensaje("transacciones-topic", transaccionId, transaccionJson);
    }
}