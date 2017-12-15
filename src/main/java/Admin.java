//import com.google.common.collect.Lists;
//import org.apache.kafka.clients.admin.*;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.config.ConfigResource;
//import org.apache.kafka.common.serialization.Serdes;
//
//import java.util.Arrays;
//import java.util.Map;
//import java.util.Properties;
//import java.util.concurrent.ExecutionException;
//
///**
// * Created by pawnow on 7/17/17.
// */
//public class Admin {
//	public static void main(String[] args) throws ExecutionException, InterruptedException {
//
//		KafkaWorker.Evn evn = KafkaWorker.Evn.PROD;
//
//		AdminClient adminClient  = AdminClient.create(KafkaWorkerString.getProperties("AdminMonior",evn));
//		DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Lists.asList("ParsedDataCaptureComponents",new String[]{"ParsedDataCaptureComponents"}));
//		Map<String, TopicDescription> topics = describeTopicsResult.all().get();
//		topics.forEach((s, topicDescription) -> topicDescription.partitions().forEach(topicPartitionInfo -> System.out.println(s + " "+topicPartitionInfo.partition() + " " + topicPartitionInfo.leader())));
////		DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Lists.asList(new ConfigResource(ConfigResource.Type.TOPIC,"sonar_event_politician"),new ConfigResource[]{}));
////		Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
//
////		KafkaWorkerString rawKafkaWorker = new KafkaWorkerString("MONITOR3", evn, "sonar_event_politician");
////		KafkaWorkerString rawKafkaWorker = new KafkaWorkerString("MONITOR", evn, "sonar_event_user");
////		KafkaWorker rawKafkaWorker = new KafkaWorker("MonitorView", evn, "sonar_event_topic");
////		KafkaWorkerString rawKafkaWorker = new KafkaWorkerString("MonitorView", evn, "test");
//		System.exit(0);
//
//	}
//}