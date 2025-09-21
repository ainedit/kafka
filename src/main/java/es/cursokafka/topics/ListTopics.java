package es.cursokafka.topics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListTopics {
	static Logger logger = LoggerFactory.getLogger(ListTopics.class);
	public static void main(String[] args) {
		logger.debug("Listing topics");
		TopicUtils.listTopics();
		logger.info("Topics Listed");
	}

}
