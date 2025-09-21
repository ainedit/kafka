package es.cursokafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.cursokafka.topics.TopicUtils;

/**
 * Producer Kafka
 *
 */
public class ProducerCallBack 
{
    public static void main( String[] args )
    {
    	Logger logger = LoggerFactory.getLogger(ProducerCallBack.class);
    	
    	String bootstrapServers ="localhost:9092";
    	String topicName="topictest";
    	
    	AdminClient admin = TopicUtils.createAdminClient("localhost","9092");
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
    	
    	logger.info("Enviando datos a "+bootstrapServers + " topic: "+ topicName);
    	for (int i =0; i<=10 ; i++) {
	    	//Creamos los registros que se enviaran
	    	ProducerRecord <String,String> record = new ProducerRecord<String, String>(topicName, "MATTAXI:"+i);
	    	try {
	    		logger.info("Enviando "+i);
	    		//Enviamos los registros de manera asincrona
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
							
						}else {//Podría haber un error en este caso si el topic por ejemplo está compactado. 
							//Cuando está compactado se necesita clave 
							//porque en un topic compactado Kafka garantiza “último valor por clave”
							logger.error("Registro no enviado " + exception.getMessage());
						}
						
					}
				});
	    	}catch (Exception e) {
				System.err.println("Error"+e.getMessage());
			}
	    	//kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic topictest --from-beginning
    	}
    	logger.info("Finalizado");
//    	producer.flush();
    	producer.close();
    	logger.info("Enviados");
    }
}
