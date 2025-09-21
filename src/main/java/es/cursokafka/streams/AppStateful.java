package es.cursokafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * Ejemplo stateful con Kafka Streams que mantiene estado local (RocksDB) 
 * y lo materializa en un state store consultable. 
 * Hace un WordCount: cuenta ocurrencias por palabra desde input-topic, 
 * publica resultados en word-counts-topic 
 * y mantiene la tabla en un store llamado word-counts-store.
 */

/*
 * # Crear topics
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic input-topic --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic word-counts-topic --partitions 3 --replication-factor 1

# Consumidor para ver resultados
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic word-counts-topic --from-beginning --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

kafka-console-producer.sh --bootstrap-server localhost:9092 --topic input-topic
> Kafka Streams es potente
> kafka kafka KAFKA
 */
public class AppStateful {

	public static void main(String[] args) {
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-wordcount-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // (Opcional) EXACTLY-ONCE
        // props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Directorio local para el estado (RocksDB)
        props.put(StreamsConfig.STATE_DIR_CONFIG, "./data/kstreams-state");

        StreamsBuilder builder = new StreamsBuilder();

        // 1) Stream fuente (texto plano por línea)
        KStream<String, String> lines = builder.stream("input-topic");

        // 2) Normaliza y tokeniza -> palabras
        KStream<String, String> words = lines
                .flatMapValues(v -> {
                    if (v == null) return Arrays.asList();
                    String norm = v.toLowerCase();
                    return Arrays.asList(norm.split("\\W+"));
                })
                .filter((k, w) -> w != null && !w.isBlank())
                // La clave pasa a ser la palabra para agrupar por clave
                .selectKey((k, word) -> word);

        // 3) Agrupa por palabra y cuenta, materializando en un store persistente
        KTable<String, Long> counts = words
                .groupByKey()
                .count(
                	Materialized.<String, Long>as(Stores.persistentKeyValueStore("word-counts-store"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                );

        // 4) Publica la tabla como stream de cambios a un topic de salida
        counts.toStream().to("word-counts-topic", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        System.out.println("TOPOLOGY:\n" + topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        // Cierre ordenado
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();

        // === Ejemplo de "Interactive Query" local ===
        // Consulta periódica del store local (solo las particiones asignadas a ESTA instancia).
        Thread queryThread = new Thread(() -> {
            try {
                // Espera a que el store esté listo
                ReadOnlyKeyValueStore<String, Long> store =
                        waitUntilStoreIsQueryable(streams, "word-counts-store");

                while (true) {
                    // Ejemplo: consulta el conteo de la palabra "kafka"
                    Long kafkaCount = store.get("kafka");
                    System.out.println("[QUERY] 'kafka' -> " + (kafkaCount == null ? 0 : kafkaCount));
                    Thread.sleep(3000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "query-thread");
        queryThread.setDaemon(true);
        queryThread.start();
    }

    private static ReadOnlyKeyValueStore<String, Long> waitUntilStoreIsQueryable(
            KafkaStreams streams, String storeName) throws InterruptedException {

        while (true) {
            try {
                return streams.store(
                        StoreQueryParameters.fromNameAndType(
                                storeName, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException ignored) {
                Thread.sleep(500);
            }
        }
    }


	
}
