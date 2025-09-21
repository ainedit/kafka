package es.cursokafka.producers;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.cursokafka.topics.TopicUtils;

/**
 * Producer Kafka
 *
 */
public class Producer 
{
    public static void main( String[] args )
    {
    	Logger logger = LoggerFactory.getLogger(Producer.class);
    			
    	String bootstrapServers ="localhost:9092";
    	String topicName="topictest";
    	
    	AdminClient admin = TopicUtils.createAdminClient(bootstrapServers);
    	if ( !TopicUtils.topicExist(admin, topicName)){
    		TopicUtils.createTopic(admin, topicName,3,1);
    	}
    	
        //Properties
    	//https://kafka.apache.org/documentation/#producerconfigs
    	Properties kafkaProps = new Properties();
    	//kafkaProps.setProperty("bootstrap.servers", bootstrapServers );
    	kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
    	
    	//Le permite como tiene que realizar la serialización a bytes ya que kafka convertirá todo a bytes
    	//Como enviamos Strings
    	kafkaProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	kafkaProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	
    	//Creamos el productor
    	KafkaProducer<String, String> producer = new KafkaProducer <String,String>(kafkaProps);
    	
    	//Creamos los registros que se enviaran
    	ProducerRecord <String,String> record = new ProducerRecord<String, String>(topicName, "MAT_TAXI:123");
    	
    	//Enviamos los registros de manera asincrona
    	logger.info("Enviando datos a "+bootstrapServers + " topic: "+ topicName);
    	try {
    		producer.send(record);
    		//has added the message to a local buffer of pending record sends
    		//Then the Producer flushes multiple messages to the Brokers based on batching configuration parameters
    		//You can also manually flush by calling the Producer method flush()
    		
    	}catch (Exception e) {
			System.err.println("Error"+e.getMessage());
		}
    	//Flush data
//    	producer.flush();
    	//Flush and close producer
    	producer.close();
    	//producer.close(Duration.ofMinutes(5));//waits up to timeout for the producer to complete the sending of all incomplete requests
    	//If the producer is unable to complete all requests before the timeout expires, this method
    	//will fail any unsent and unacknowledged records immediately
    	
    	logger.info("Enviado");
    }
}
