package es.cursokafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.cursokafka.producers.interceptors.StockProducerInterceptor;

/**
 * Producer Kafka
 *
 */
public class ProducerPropertiesHighThroughput 
{
	static String bootstrapServers ="localhost:9093";
	static String topicName="topic-test-acks";
	//kafka-topics --zookeeper localhost:2181 --create --topic topic-test-highthroughput --partitions 3 --replication-factor 3 --config min.insync.replicas=2
	//kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic topic-test-highthroughput --from-beginning
    public static void main( String[] args ) throws InterruptedException
    {
    	Logger logger = LoggerFactory.getLogger(ProducerPropertiesHighThroughput.class);
    	
    	
    	// create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();
    	
    	
    	for (int i =0; i<=10 ; i++) {
	    	//Creamos los registros que se enviaran
    		
    		String valor = "Enviando datos comprimidos"+i;
    		String clave = "key_"+i;
    		Thread.sleep(15000);
    		logger.info("Enviando datos comprimidos a "+bootstrapServers + " topic: "+ topicName + " clave: "+clave);
	    	ProducerRecord <String,String> record = new ProducerRecord<String, String>(topicName, clave, valor);
	    	
	    	try {
	    		
	    		//Enviamos los registros de manera SINCRONA!!! con el get() del final
	    		producer.send(record, new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						//Cada vez que un registro se envia satisfactoriamente o se envia una excepci�n
						if (exception==null) {
							logger.info("Recibida metainformación \n" + 
								"Topic: " + metadata.topic()+ "\n" +
								"Partition: " + metadata.partition()+ "\n" +
								"Offset: " + metadata.offset()+ "\n" +
								"Timestamp: " + metadata.timestamp()+ "\n" );
							
						}else {
							logger.error("Registro no enviado " + exception.getMessage());
						}
					}
				}).get();//NO HACER ESTO EN PRODUCCION!!!!
	    	}catch (Exception e) {
				System.err.println("Error"+e.getMessage());
			}
    	}
//    	producer.flush();
    	producer.close();
    	logger.info("Enviados");
    }
    
    public static KafkaProducer<String, String> createKafkaProducer(){

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

        //ACKs
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, StockProducerInterceptor.class.getName());

        
        //high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "200");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
