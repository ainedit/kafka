package es.cursokafka.registry;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import demo.User;

public class ConsumerApp {

	private static final String TOPIC = System.getProperty("topic", "usersavro");
	private static final String BOOTSTRAP = System.getProperty("bootstrap", "localhost:9092");
	private static final String SCHEMA_REGISTRY = System.getProperty("schemaRegistry", "http://localhost:8081");

	public static void main(String[] args) {
		Properties c = new Properties();
		c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
		c.put(ConsumerConfig.GROUP_ID_CONFIG, "users-avro-consumer");
		c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		c.put("schema.registry.url", SCHEMA_REGISTRY);
		c.put("specific.avro.reader", "true"); // devolver demo.User (SpecificRecord)

		try (KafkaConsumer<String, User> consumer = new KafkaConsumer<>(c)) {
			consumer.subscribe(Collections.singletonList(TOPIC));
			System.out.println("Esperando mensajes en " + TOPIC + " ... (Ctrl+C para salir)");
			while (true) {
				ConsumerRecords<String, User> records = consumer.poll(Duration.ofSeconds(1));
				for (ConsumerRecord<String, User> r : records) {
					System.out.println(r.toString());
					User u = r.value();
					System.out.printf("Recibido key=%s value=User{id=%s, name=%s} @ %s-%d@%d%n", r.key(), u.getId(),
							u.getName(), r.topic(), r.partition(), r.offset());
				}
				// commit auto por defecto; si prefieres manual: consumer.commitSync();
			}
		}
	}
}