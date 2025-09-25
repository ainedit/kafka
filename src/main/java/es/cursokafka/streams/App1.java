package es.cursokafka.streams;

/*
 * # Crear topics
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic input-topic --partitions 1 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic output-topic --partitions 1 --replication-factor 1

# Consumidor para ver resultados (output)
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output-topic --from-beginning


# Productor (input)
kafka-console-producer --bootstrap-server localhost:9092 --topic input-topic
# Escribe:  hola
# Verás en el consumidor:  HOLA
 */

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
/*
Serdes: Serializers/Deserializers. Indican cómo convertir entre bytes (lo que maneja Kafka) y objetos Java (String, Integer, etc.).
StreamsBuilder: se usa para construir la topología de flujo (el "pipeline" de operaciones sobre los datos).
Topology: representa la estructura final de procesamiento de la aplicación.
KafkaStreams: arranca la aplicación, conecta con Kafka y empieza a procesar.
KStream: representa un stream de registros (clave, valor) que fluyen desde un topic.
*/
public class App1 {

	public static void main(String[] args) {
		 // --- Propiedades básicas ---
        Properties props = new Properties();
        props.put("application.id", "kstreams-uppercase-app");
        props.put("bootstrap.servers", "localhost:9092"); // ajusta si usas Docker/VM
        props.put("default.key.serde", Serdes.StringSerde.class.getName());
        props.put("default.value.serde", Serdes.StringSerde.class.getName());
        // Opcionales: commit + cierre más ágiles en desarrollo
        props.put("processing.guarantee", "at_least_once");
        props.put("commit.interval.ms", "1000");
        /*
         *  application.id: Identificador único de tu aplicación Kafka Streams. Sirve para agrupar instancias y gestionar offsets internos.
			bootstrap.servers: dirección de tu cluster Kafka (el broker).
			default.key.serde / default.value.serde: definimos que tanto las claves como los valores son String. Kafka Streams lo usará automáticamente para serializar/deserializar.
			processing.guarantee: at_least_once garantiza que ningún mensaje se pierde, aunque puede procesarse dos veces en casos raros.
			commit.interval.ms: cada cuánto guarda offsets y estados (aquí, cada segundo, útil para desarrollo).
         */
        
        // --- Topología: input -> toUpperCase -> output ---
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("input-topic");
        input.mapValues(v -> v == null ? null : v.toUpperCase())
        	.mapValues(v->v.concat(" dato"))
             .to("output-topic");

        Topology topology = builder.build();
        System.out.println(topology.describe());
        /*builder.stream("input-topic"): crea un flujo (KStream) que leerá todos los mensajes del topic input-topic.
		mapValues(...): transforma solo el valor de cada mensaje (dejando intacta la clave). En este caso:
		si el valor es null, lo dejamos como null.
		si no, lo convertimos a mayúsculas (v.toUpperCase()).
		to("output-topic"): envía el resultado al topic output-topic.
		topology.describe(): imprime un “mapa” de la topología en consola, útil para depurar.
		*/
        
        
        
        KafkaStreams streams = new KafkaStreams(topology, props);

        // Cierre ordenado
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
        
        /*
         * KafkaStreams streams = new KafkaStreams(...): crea la instancia de Kafka Streams con la topología y la configuración.
			addShutdownHook: asegura que cuando pares la app (CTRL+C), se cierre bien y guarde estado/offsets.
			streams.start(): arranca la aplicación → empieza a leer del input-topic, aplicar la transformación, y escribir en output-topic.
         */

	}

}
