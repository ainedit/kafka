package es.cursokafka.topics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteTopic {
	static Logger logger = LoggerFactory.getLogger(DeleteTopic.class);
	
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		AdminClient admin = TopicUtils.createAdminClient();
		//args = new String[]{"topic-ejercicio"};
		if (args.length==0) {
			logger.info("Deleting default topic \"topictest\"");
			ArrayList<String> topicsToDelete = new ArrayList<String>();
			topicsToDelete.add("topictest");
			TopicUtils.deleteTopic(admin, topicsToDelete);

		}else if (args[0].equals("all")) {
			logger.info("Deleting all topics");
			TopicUtils.deleteAllTopics(admin);
		}else {
			logger.info("Deleting topics "+Arrays.asList(args));
			TopicUtils.deleteTopic(admin,Arrays.asList(args));
		}
		Thread.sleep(5000);
		logger.info("Deleting topics finished");
		System.out.println("Termina");
	}
}