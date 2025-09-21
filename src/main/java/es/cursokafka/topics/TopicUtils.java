package es.cursokafka.topics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicUtils {
	static Logger logger = LoggerFactory.getLogger(TopicUtils.class);
	
	public static AdminClient createAdminClient(String url, String port) {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, url.concat(":").concat(port));
		AdminClient admin = AdminClient.create(config);
		
		return admin;
	}
	
	public static AdminClient createAdminClient(String bootstrapserver) {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
		AdminClient admin = AdminClient.create(config);
		
		return admin;
	}
	
	public static AdminClient createAdminClient(List<String> bootstrapservers) {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
		AdminClient admin = AdminClient.create(config);
		
		return admin;
	}
	
	public static AdminClient createAdminClient() {
		List<String> bootStrapServers = new ArrayList<String>();
		bootStrapServers.add("localhost:9092");
		bootStrapServers.add("localhost:9093");
		bootStrapServers.add("localhost:9094");
		
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		AdminClient admin = AdminClient.create(config);
		
		return admin;
	}
	
	public static void deleteAllTopics(AdminClient admin) {
		logger.info("Deleting all topics");
		List<String> topicNames = getTopicsName(admin);
		topicNames.stream().filter(topicName->!topicName.startsWith("_")).forEach(topicName->deleteTopic(admin,topicName));
		//deleteTopic(admin,topicNames);
	}
	
	public static void deleteTopic(AdminClient admin, List<String> topicsToDelete) {
		logger.info("Deleting topics "+topicsToDelete);
		try {
			admin.deleteTopics(topicsToDelete);
		} catch (Exception e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	public static void deleteTopic(AdminClient admin, String topicToDelete) {
		logger.info("Deleting topic "+topicToDelete);
		List<String> topicsToDelete = new ArrayList<String>();
		topicsToDelete.add(topicToDelete);
		try {
			admin.deleteTopics(topicsToDelete);
		} catch (Exception e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	public static void createTopic (AdminClient admin, String topicName) {  
		createTopic(admin,topicName,1,1);
	}
	
	public static void createTopic (AdminClient admin, String topicName, int noOfPartitions, int noOfReplication) {
		createTopic(admin,topicName, noOfPartitions,noOfReplication , false );
	}
	
	public static void createTopic (AdminClient admin, String topicName, int noOfPartitions, int noOfReplication, boolean compact) {
		logger.info("Creating topic "+topicName);
		
		// 2) Definir el topic y (opcionalmente) sus configs
		Map<String, String> newTopicConfig = new HashMap<>();
		if (compact) {
			logger.debug("Creating compact topic with LZ4");
			newTopicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
			newTopicConfig.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4");
		}
		newTopicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "604800000");    // 7 días
	      // Si tienes 3 brokers y quieres durabilidad:
	      // cfg.put("min.insync.replicas", "2");
		
		NewTopic newTopic = new NewTopic(topicName, noOfPartitions, (short) noOfReplication);
		newTopic.configs(newTopicConfig);
		
		// 3) Crear (o validar primero)
	    CreateTopicsOptions opts = new CreateTopicsOptions()
	          .validateOnly(false)                // pon true para "probar" sin crear
	          .timeoutMs(15_000);
		
		try {
			admin.createTopics(Collections.singleton(newTopic));
			
			//admin.createTopics(Collections.singleton(newTopic), opts);
            
			
		} catch (Exception e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	
	/**
	 * 
	 * @param admin
	 * @param topicName
	 * @param noOfPartitions
	 * @param noOfReplication
	 * @return
	 */
	public static boolean topicExist (AdminClient admin, String topicName) {
        return TopicUtils.getTopicsName(admin).contains(topicName);
	}
	
	public static void listTopics (AdminClient admin) {
		// listing
		logger.info("-- listing --");
		try {
			admin.listTopics().names().get().forEach(System.out::println);
		}catch (InterruptedException e) {
			logger.error("Error" + e.getMessage());
		}catch (ExecutionException e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	public static void listTopics () {
		// listing
		logger.info("-- listing --");
		try {
			createAdminClient().listTopics().names().get().forEach(System.out::println);
		}catch (InterruptedException e) {
			logger.error("Error" + e.getMessage());
		}catch (ExecutionException e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	public static List<String> getTopicsName (AdminClient admin) {
		// listing
		List<String> topics = new ArrayList<String>();
		logger.info("-- getting Topics names --");
		try {
			
			admin.listTopics().names().get().forEach((name)->topics.add(name));
		}catch (InterruptedException e) {
			logger.error("Error" + e.getMessage());
		}catch (ExecutionException e) {
			logger.error("Error" + e.getMessage());
		}
		return topics;
	}
}
