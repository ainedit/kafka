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

/**
 * Producer Kafka
 *
 */
public class ProducerPropertiesAcks 
{
	static String bootstrapServers ="localhost:9092";
	static String topicName="topic-test-acks";
	//Crear Topic
	//Version <2.2
	//kafka-topics --zookeeper zoo1:2181 --create --topic topic-test-acks --partitions 3 --replication-factor 3 --config min.insync.replicas=2
	//kafka-topics --zookeeper zoo1:2181 --describe --topic topic-test-acks
	
	//Version >3
	//kafka-topics --bootstrap-server localhost:9092 --create --topic topic-test-acks --partitions 3 --replication-factor 3 --config min.insync.replicas=2
	//kafka-topics --bootstrap-server localhost:9092 --describe --topic topic-test-acks

	//Al arrancar parar contenedores hasta provocar excepci�n NOT_ENOUGH_REPLICAS
	//docker container stop kafka-kafka1-1
	private static final Logger logger = LoggerFactory.getLogger(ProducerPropertiesAcks.class);
    public static void main( String[] args ) throws InterruptedException
    {
    	// create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();
    	    	
    	for (int i =0; i<=20 ; i++) {
	    	//Creamos los registros que se enviaran
    		Thread.sleep(10000);
    		String valor = "Enviando datos "+i;
    		String clave = "key_"+i;
    		logger.info("Enviando datos a "+bootstrapServers + " topic: "+ topicName + " clave: "+clave);
	    	ProducerRecord <String,String> record = new ProducerRecord<String, String>(topicName, clave, valor);
	    	
	    	try {
	    		//Enviamos los registros de manera SINCRONA!!! con el get() del final
	    		producer.send(record, new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						//Cada vez que un registro se envia satisfactoriamente o se envia una excepción
						if (exception==null) {
							logger.info("Recibida metainformaci�n \n" + 
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
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");//Default Integer.MAX_VALUE
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");//Default 100 ms
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"30000");//Default 120000 
        
        //Peticiones en paralelo que el productor puede hacer
        //Con valor 1 me aseguro que aunque esten fallando envios el orden se mantiene pero el rendimiento cae
        //Con valor 5 (por defecto) me aseguro mayor rendimiento pero no me aseguro del orden de envio si alguno falla
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        
        //Habilitamos productor idempotente que garantiza una ejecuci�n estable
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

         // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
