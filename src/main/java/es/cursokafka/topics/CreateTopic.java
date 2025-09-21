package es.cursokafka.topics;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTopic {
	
	private final static Logger logger = LoggerFactory.getLogger(CreateTopic.class);
	
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		
		//args = new String[]{"topicTaxi;3;2","topicTruck;3;3"};
		//args = new String[]{"topic-ejercicio;3;2"};
		AdminClient admin = TopicUtils.createAdminClient();
		if(args.length==0) {
			String topicName="";
			Scanner scan = new Scanner(System.in);
			System.out.println("Introduce nombre del topic (defecto topictest)");
			topicName= scan.nextLine();
			int numPartitions = 3;
			int replicationFactor= 1;
			
			if (!topicName.isBlank() && !topicName.isEmpty()) {
				System.out.println("Introduce número de particiones");
				numPartitions = scan.nextInt();
				System.out.println("Introduce factor de replicación");
				replicationFactor = scan.nextInt();
				logger.info("Creating topic "+topicName+","+numPartitions+","+replicationFactor);
			}else {
				logger.info("Creating default topic \"topictest\""+numPartitions+","+replicationFactor);
				topicName="topictest";
			}
			
	    	if ( !TopicUtils.topicExist(admin, topicName)){
	    		TopicUtils.createTopic(admin, topicName,numPartitions,replicationFactor);
	    	}else {
	    		logger.warn(topicName + " already exists");
	    	}
	    	
		}else {
			for (String topic : args) {
				if (topic.contains(";")) {
					String[] topicData = topic.split(";");
					logger.info("Creating topic with data "+Arrays.toString(topicData));
					TopicUtils.createTopic(admin, topicData[0], Integer.parseInt(topicData[1]) , Integer.parseInt(topicData[2]));
				}else {
					logger.info("Creating topic with name "+args[0]);
					TopicUtils.createTopic(admin, args[0]);
				}
			}
			
		}
		Thread.sleep(1000);
		logger.info("Creating topics finished");
	}
}