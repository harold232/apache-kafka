# APACHE KAFKA - DEMO

### Grupo 4
### Curso: Taller de Aplicaciones Sociales

## Requisitos
- *Java 17+*
- *Apache Kafka* (Docker)
- *Maven* para la gestión de dependencias

## Configuración
1. Asegúrate de tener un clúster de Kafka y Zookeeper en ejecución. Puedes usar docker-compose para configurarlo.
2. Configura los topics necesarios en Kafka antes de ejecutar las aplicaciones.

bash
```
docker-compose up -d
```

## Verificar Contenedores Docker
Para verificar que los contenedores están corriendo:

bash
```
docker ps
```

Para detener los contenedores:

bash
```
docker-compose down
```

## Ejecutar desde (MAVEN)

En caso tenga maven instaldo

Sigue estos pasos para ejecutar las tres clases principales del proyecto:

### 1. Productor Kafka
Ejecuta el productor para enviar mensajes a un topic:



bash
```
mvn compile exec:java -Dexec.mainClass="com.harold.KafkaProducerApp"
```

Ejecuta el consumer para recibir mensajes a un topic:

bash
```
mvn compile exec:java -Dexec.mainClass="com.harold.KafkaConsumerApp"
```

Ejecuta el streams para el procesamiento de mensajes en un topic:

bash
```
mvn compile exec:java -Dexec.mainClass="com.harold.KafkaStreamsApp"
```

## Ejecutar desde un IDE

Si prefieres ejecutar las clases directamente desde un IDE en lugar de usar Maven, sigue estos pasos:

### IntelliJ IDEA
1. Abre el proyecto en IntelliJ IDEA.
2. Asegúrate de que el SDK de Java 17 esté configurado en el proyecto.
3. Navega a la clase que deseas ejecutar (KafkaProducerApp, KafkaConsumerApp o KafkaStreamsApp).
4. Haz clic derecho en la clase y selecciona *Run 'NombreDeLaClase'*.

### VS Code
1. Abre el proyecto en VS Code.
2. Instala la extensión *Java Extension Pack* si no la tienes instalada.
3. Asegúrate de que el entorno de Java esté configurado correctamente.
4. Abre la clase que deseas ejecutar (KafkaProducerApp, KafkaConsumerApp o KafkaStreamsApp).
5. Haz clic en el botón *Run* en la parte superior derecha o presiona F5.

### NetBeans
1. Abre el proyecto en NetBeans.
2. Asegúrate de que el JDK 17 esté configurado en el proyecto.
3. Haz clic derecho en la clase que deseas ejecutar (KafkaProducerApp, KafkaConsumerApp o KafkaStreamsApp).
4. Selecciona *Run File* para ejecutar la clase.