package es.cursokafka.registry;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import demo.User;

//import demo.User;

public class ProducerApp {
	private static final String TOPIC = System.getProperty("topic", "usersavro");
	private static final String BOOTSTRAP = System.getProperty("bootstrap", "localhost:9092");
	private static final String SCHEMA_REGISTRY = System.getProperty("schemaRegistry", "http://localhost:8081");

	public static void main(String[] args) throws Exception {
		//ensureTopic(TOPIC, 3, (short) 1); // cambia RF=3 si tienes 3 brokers

		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
		p.put(ProducerConfig.ACKS_CONFIG, "all");
		p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
		p.put("schema.registry.url", SCHEMA_REGISTRY);

		try (KafkaProducer<String, User> producer = new KafkaProducer<>(p)) {
			sendUser(producer, "u4", "Eva");
			sendUser(producer, "u5", "Juan");
			sendUser(producer, "u6", "Marta");
			producer.flush();
			System.out.println("Mensajes enviados a " + TOPIC);
		}
	}

	private static void sendUser(KafkaProducer<String, User> producer, String id, String name)
			throws ExecutionException, InterruptedException {
		User u = User.newBuilder().setId(id).setName(name).build();
		ProducerRecord<String, User> rec = new ProducerRecord<>(TOPIC, id, u);
		RecordMetadata meta = producer.send(rec).get();
		System.out.printf("Enviado key=%s -> %s-%d@%d%n", id, meta.topic(), meta.partition(), meta.offset());
	}

	private static void ensureTopic(String topic, int partitions, short rf) throws Exception {
		Properties ap = new Properties();
		ap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
		ap.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
		try (AdminClient admin = AdminClient.create(ap)) {
//			NewTopic nt = new NewTopic(topic, partitions, rf);
//			CreateTopicsResult res = admin.createTopics(Collections.singleton(nt));
//			try {
//				res.all().get();
//				System.out.println("Topic creado: " + topic);
//			} catch (ExecutionException e) {
//				if (e.getCause() instanceof TopicExistsException) {
//					System.out.println("Topic ya existe: " + topic);
//				} else {
//					throw e;
//				}
//			}
			System.out.println(topic);
			// breve describe
			admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get()
					.forEach((t, d) -> System.out.println("Topic " + t + " particiones=" + d.partitions().size()));
		}
	}
}
