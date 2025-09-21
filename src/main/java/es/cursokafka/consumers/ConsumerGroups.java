package es.cursokafka.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerGroups {

	public static void main(String[] args) {
		
		//Activar logs de consumer de Kafka para ver rebalanceo de particiones de los consumidores
		//log4j.logger.org.apache.kafka.clients.consumer
		//Levantar dos y tres consumidores y ver como reasigna las particiones
		//Añadir un cuarto
		//Parar dos de ellos
		Logger logger = LoggerFactory.getLogger(ConsumerGroups.class);
		
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
    	kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//"earliest","latest","none" (none si no se guardan)
    	
    	//Al crear un nuevo consumidor de Kafka, podemos configurar la estrategia que se utilizará para asignar las particiones entre las instancias del consumidor.
    	kafkaProps.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());//RangeAssignor(defecto), RoundRobinAssignor, StickyAssignor
    	
    	kafkaProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"5");
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
    			logger.info("Recibido registro: \n" +
    					"Record: " + record.key()+ "/" + record.value() + "\t" +
						"Partition: " + record.partition()+ "\t" +
						"Offset: " + record.offset()+ "\t"  );
			}
    		try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	// kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group CLIENTE4 --describe
	}

}
