package es.cursokafka.consumers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Consumer {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(Consumer.class);
		
		//https://kafka.apache.org/documentation/#consumerconfigs
		String bootstrapServers ="localhost:9092";
    	String topicName="topic-test";
    	String groupId = "CLIENTE4";
    	Properties kafkaProps = new Properties();
    	//kafkaProps.setProperty("bootstrap.servers", bootstrapServers );
    	kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
    	
    	
    	//En este tiene que realizar la deserialización a de bytes al objeto que se haya mandado
    	//Como enviamos Strings
    	kafkaProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	kafkaProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
    	kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//"earliest","latest","none" (none si prefieres establecer el desplazamiento inicial)
    	//none: throw exception to the consumer if no previous offset is found for the consumer's group
    	
    	//Creamos el consumidor
    	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);
    	
    	
    	//Subscribimos el consumidor al topic
    	consumer.subscribe(Collections.singleton(topicName));
    	//consumer.subscribe(Arrays.asList("topic-test","topic-test2"));
    
    	//Obtención de la información
    	while(true) {
    		//consumer.poll(2000);//Deprecated desde la versión 2.0 de kafka
    		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
    		
    		for (ConsumerRecord<String, String> record : records) {
    			if (record.topic().equals("topic-taxi")){
    				
    			}else {
    				
    			}
    			logger.info("Recibido registro: \n" +
    					"Record: " + record.key()+ "/" + record.value() + "\t" +
						"Partition: " + record.partition()+ "\t" +
						"Offset: " + record.offset()+ "\t"  );
			}
    	}
    	
    	//Ejecutar: kafka-consumer-groups --bootstrap-server localhost:9092 --group CLIENTE4 --describe
    	
	}

}
