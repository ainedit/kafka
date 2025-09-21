package es.cursokafka.producers.interceptors;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StockProducerInterceptor implements ProducerInterceptor<String, String> {

	Logger logger = LoggerFactory.getLogger(StockProducerInterceptor.class);
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		logger.info("onSend key:"+record.key());
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		logger.info("onAcknowledgement");
		
		if(exception==null) {
			logger.info("ACK " + metadata.toString());
		}else {
			logger.error("ACK ERROR " + exception.getMessage());
			exception.printStackTrace();
		}
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	
}
