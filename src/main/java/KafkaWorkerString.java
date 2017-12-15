import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by pawnow on 7/17/17.
 */
public class KafkaWorkerString {

	private String groupId;
	private KafkaWorker.Evn evn;
	private String topicName;

	public KafkaWorkerString(String groupId, KafkaWorker.Evn evn, String topicName) {
		this.groupId = groupId;
		this.evn = evn;
		this.topicName = topicName;
	}

	public void workWithWorker(Function<ConsumerRecord<String, String>, Boolean> consumerFuction ){
		workWithWorker(consumerFuction,	System.currentTimeMillis());
	}
	public void workWithWorker(Function<ConsumerRecord<String, String>, Boolean> consumerFuction,Long startTimestamp ) {
		Properties configProperties = getProperties(groupId, evn);
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
		try {

			List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topicName);
			List<TopicPartition> topicPartitionList = partitionInfoList.stream().map(partitionInfo -> new TopicPartition(partitionInfo
					.topic(), partitionInfo.partition())).collect(Collectors.toList());
			Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
			topicPartitionList.forEach(topicPartition -> topicPartitionLongMap.put(topicPartition, startTimestamp));
			Set<Map.Entry<TopicPartition, OffsetAndTimestamp>> set = kafkaConsumer.offsetsForTimes(topicPartitionLongMap).entrySet();
			kafkaConsumer.assign(topicPartitionList);
			 kafkaConsumer.seekToBeginning(topicPartitionList);
			set.forEach(entry -> kafkaConsumer.seek(entry.getKey(), entry.getValue().offset()));
			stop: while (true) {
				try {
					ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
 					for (ConsumerRecord<String, String> record : records) {
						if (consumerFuction.apply(record)) {
							break stop;
						}
					}
				}catch (Exception e){
					e.printStackTrace();
				}

			}
		} finally {
			kafkaConsumer.close();

		}
	}

	public static Properties getProperties(String groupId, KafkaWorker.Evn evn) {
		Properties configProperties = new Properties();
		switch (evn) {
			case PROD:
				configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
						"dataflow-kafka01.gazeta.pl:6667,dataflow-kafka03.gazeta.pl:6667");
				configProperties.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" +
						"\t\t\t\t\tuseKeyTab=true\n" + "\t\t\t\t\tkeyTab=\"/etc/security/keytabs/prod-dataocean.keytab\"\n" +
						"\t\t\t\t\tstoreKey=true\n" + "\t\t\t\t\tuseTicketCache=false\n" + "\t\t\t\t\tserviceName=\"kafka\"\n" +
						"\t\t\t\t\tprincipal=\"prod-dataocean@AGORA.PL\";");

				break;
			case TEST:
				configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
						"testflow-kafka04.gazeta.pl:6667,testflow-kafka02.gazeta.pl:6667");
				configProperties.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" +
						"\t\t\t\t\tuseKeyTab=true\n" + "\t\t\t\t\tkeyTab=\"/etc/security/keytabs/test-testocean.keytab\"\n" +
						"\t\t\t\t\tstoreKey=true\n" + "\t\t\t\t\tuseTicketCache=false\n" + "\t\t\t\t\tserviceName=\"kafka\"\n" +
						"\t\t\t\t\tprincipal=\"test-testocean@AGORA.PL\";");

				break;
		}
		//		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "testflow-kafka04.gazeta.pl:6667,testflow-kafka02.gazeta.pl:6667");
		//		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dataflow-kafka01.gazeta.pl:6667,dataflow-kafka03.gazeta.pl:6667");
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		//              configProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
		configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple"+UUID.randomUUID().toString());
		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		configProperties.put("security.protocol", "SASL_PLAINTEXT");

		return configProperties;
	}


}
