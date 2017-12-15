import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by pawnow on 7/17/17.
 */
public class KafkaWorkerAutoRebalance {

	private String groupId;
	private KafkaWorker.Evn evn;
	private String topicName;

	public KafkaWorkerAutoRebalance(String groupId, KafkaWorker.Evn evn, String topicName) {
		this.groupId = groupId;
		this.evn = evn;
		this.topicName = topicName;
	}

	public void workWithWorker(Function<ConsumerRecord<String, String>, Boolean> consumerFuction ){
		workWithWorker(consumerFuction,	System.currentTimeMillis());
	}
	public void workWithWorker(Function<ConsumerRecord<String, String>, Boolean> consumerFuction,Long startTimestamp ) {
		Properties configProperties = KafkaWorkerString.getProperties(groupId, evn);
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
		try {

//			List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topicName);
//			List<TopicPartition> topicPartitionList = partitionInfoList.stream().map(partitionInfo -> new TopicPartition(partitionInfo
//					.topic(), partitionInfo.partition())).collect(Collectors.toList());
//			Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
//			topicPartitionList.forEach(topicPartition -> topicPartitionLongMap.put(topicPartition, startTimestamp));
//			Set<Map.Entry<TopicPartition, OffsetAndTimestamp>> set = kafkaConsumer.offsetsForTimes(topicPartitionLongMap).entrySet();
//			kafkaConsumer.assign(topicPartitionList);
			kafkaConsumer.subscribe(Lists.asList(topicName, new String[0]));

//			topicPartitionList.forEach(topicPartition -> kafkaConsumer.seek(topicPartition, 0));
//			set.forEach(entry -> kafkaConsumer.seek(entry.getKey(), entry.getValue().offset()));
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

}
