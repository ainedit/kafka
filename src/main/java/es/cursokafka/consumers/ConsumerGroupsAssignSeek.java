package es.cursokafka.consumers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerGroupsAssignSeek {
	//No asociamos a un grupo ni subscribimos a un topic
	public static void main(String[] args) {
		

		Logger logger = LoggerFactory.getLogger(ConsumerGroupsAssignSeek.class);
		
		//https://kafka.apache.org/documentation/#consumerconfigs
		String bootstrapServers ="localhost:9092";
    	String topicName="topic-test";

    	Properties kafkaProps = new Properties();
    	//kafkaProps.setProperty("bootstrap.servers", bootstrapServers );
    	kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
    	
    	
    	//En este tiene que realizar la deserialización a de bytes al objeto que se haya mandado
    	//Como enviamos Strings
    	kafkaProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	kafkaProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//"earliest","latest","none" (none si no se guardan)
    	
    	
    	//Creamos el consumidor
    	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);
    	
    	
    	//NO Subscribimos el consumidor al topic
    	//assign y seek se usan para repetir información o coger un mensaje específico
    	
    	//assign
    	TopicPartition partitionToReadFrom = new TopicPartition(topicName,1);
    	consumer.assign(Arrays.asList(partitionToReadFrom));
    	
    	//seek
    	long offsetToReadFrom= 15;
    	consumer.seek(partitionToReadFrom, offsetToReadFrom);

		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
		
		for (ConsumerRecord<String, String> record : records) {
			logger.info("Recibido registro: \n" +
					"Record: " + record.key()+ "/" + record.value() + "\t" +
					"Partition: " + record.partition()+ "\t" +
					"Offset: " + record.offset()+ "\t"  );
		}
    	
    	logger.info("Termina");

	}

}
