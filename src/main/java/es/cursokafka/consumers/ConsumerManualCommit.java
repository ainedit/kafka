package es.cursokafka.consumers;

import java.time.Duration;
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

/**
 * Deshabilitado el auto commit
 * @author joseluisllorente
 *
 */
public class ConsumerManualCommit {

	public static void main(String[] args) {
		
		Logger logger = LoggerFactory.getLogger(ConsumerManualCommit.class);
		
		//https://kafka.apache.org/documentation/#consumerconfigs
		String bootstrapServers ="localhost:9092";
    	String topicName="topic-test";
    	String groupId = "CLIENTE5";
    	Properties kafkaProps = new Properties();
    	//kafkaProps.setProperty("bootstrap.servers", bootstrapServers );
    	kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
    	
    	
    	//En este tiene que realizar la deserializaciÃ³n a de bytes al objeto que se haya mandado
    	//Como enviamos Strings
    	kafkaProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	kafkaProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    	kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
    	kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//"earliest","latest","none" (none si no se guardan)
    	
    	//Deshabilito autocommit
    	kafkaProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
    	//Cambio para ver como se commitan de 3 en 3
    	kafkaProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"3");
    	
    	//Creamos el consumidor
    	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);
    	
    	
    	//Subscribimos el consumidor al topic
    	consumer.subscribe(Collections.singleton(topicName));
    	//consumer.subscribe(Arrays.asList("topic-test","topic-test2"));
    
    	//Obtención de la información
    	while(true) {
    		//consumer.poll(2000);//Deprecated desde la versiÃ³n 2.0 de kafka
    		
    		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    		int numRecords = records.count();
    		logger.info("Recibidos "+ numRecords + " registros");
    		
    		for (ConsumerRecord<String, String> record : records) {
    			//consumer.commitSync(Collections.singletonMap(record.partition(), new OffsetAndMetadata(record.offset() + 1)));
    			String id = record.topic() + "_" + record.partition() + "_" + record.offset();
    			
    			
    			logger.info("Recibido registro: \n" +
    					"Record: " + record.key()+ "/" + record.value() + "\t" +
						"Partition: " + record.partition()+ "\t" +
						"Offset: " + record.offset()+ "\t"  );
    			
    			//consumer.commitSync();
    			
			}
    		
    		
			
    		if(numRecords>0) {
    			//Prueba parar y volver a lanzar, como no se ha hecho commit se vuelven a leer
    			try {
    				logger.info("Esperando 5 segs");
    				Thread.sleep(5000);
    			} catch (InterruptedException e) {
    				e.printStackTrace();
    			}
    			
    			
    			//Note that using the automatic commits gives you at least once processing since the consumer
    			//guarantees that offsets are only committed for messages which have been returned to the application.
	    		logger.info("Commiting offsets ...");
	    		consumer.commitSync();
	    		logger.info("Offsets commited");
//	    		try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
    		}
    		//Parar antes de que acabe y visualizar con kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group CLIENTE5
    	}
    	
	}

}
