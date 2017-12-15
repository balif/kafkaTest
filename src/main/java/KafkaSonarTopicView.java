import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by pawnow on 7/17/17.
 */
public class KafkaSonarTopicView {
	public static void main(String[] args) throws ExecutionException, InterruptedException {

		KafkaWorker.Evn evn = KafkaWorker.Evn.TEST;

//		AdminClient adminClient  = AdminClient.create(KafkaWorkerString.getProperties("AdminMonior",evn));
//		DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Lists.asList("sonar_event_politician",new String[]{"sonar_event_user"}));
//		Map<String, TopicDescription> topics = describeTopicsResult.all().get();
//
//		DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Lists.asList(new ConfigResource(ConfigResource.Type.TOPIC,"sonar_event_politician"),new ConfigResource[]{}));
//		Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();

//		KafkaWorkerString rawKafkaWorker = new KafkaWorkerString("MONITOR3", evn, "sonar_event_politician");
//		KafkaWorkerString rawKafkaWorker = new KafkaWorkerString("MONITOR", evn, "sonar_event_user");
//		KafkaWorker rawKafkaWorker = new KafkaWorker("MonitorView", evn, "sonar_event_topic");
//		KafkaWorkerString rawKafkaWorker = new KafkaWorkerString("MonitorView", evn, "test");
		Properties prop = KafkaWorkerString.getProperties("asdasdavwefvwv", evn);
		prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
		KafkaConsumer<String, Long> kafkaConsumer = new KafkaConsumer<>(prop);
		kafkaConsumer.subscribe(Arrays.asList("ks-test-application2-countPV-changelog"));
		System.out.println("test");
		new Thread(() -> {
			while(2>1) {
				kafkaConsumer.poll(100).forEach(record -> {
					System.out.println(record.timestampType() + " " + record.timestamp() + " key: " + record.key() + " value: " + record.value());


				});
			}

		}).start();
	}
}