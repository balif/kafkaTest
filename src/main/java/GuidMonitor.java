import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by pawnow on 7/17/17.
 */
public class GuidMonitor {
	public static void main(String[] args) {
//		Properties properties = KafkaWorkerString.getProperties("rawDataMonitorTest", KafkaWorker.Evn.PROD);
//		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
//				"squid-kafka01.gazeta.pl:9092,squid-kafka02.gazeta.pl:9092,squid-kafka03.gazeta.pl:9092");
//		properties.put("security.protocol","PLAINTEXT");
//		KafkaConsumer<String,String> oldKafka = new KafkaConsumer<String, String>(properties);
//
//
//		oldKafka.subscribe(Lists.asList("DataCaptureComponents", new String[]{}));
//
//			oldKafka.poll(1000).forEach(stringStringConsumerRecord -> System.out.println(stringStringConsumerRecord.offset()));
////
		KafkaWorker.Evn evn = KafkaWorker.Evn.TEST;
		KafkaWorker rawKafkaWorker = new KafkaWorker("GUIDMONITOR", KafkaWorker.Evn.PROD, "DataCaptureComponents");
		PrintStream ps = System.out;
		long[] rawCount = new long[]{0};
		long[] parsedCount = new long[]{0};
		Set<String> guidSet = Sets.newConcurrentHashSet();
		new Thread(() -> {
			rawKafkaWorker.workWithWorker(record -> {
				rawCount[0]++;
				String guid = record.value().get("guid").asText();
				if (guid.equalsIgnoreCase("65576b469e8246ae3322c30d")) {
					System.out.println("Raw " + record.value().toString());
					guidSet.add(guid);
				}
				if(record.offset()%100==0) {
					guidSet.add(guid);
				}
				if(record.offset()%10000==0){
					Long timestamp = record.value().get("hitTimestamp").asLong();
					ps.println(rawCount[0] + " PV raw timestamp "  +timestamp + " diff "+ (System.currentTimeMillis()-timestamp));
//					System.out.println(record.value().toString());


				}
				return false;
			});
		}).start();
		KafkaWorker parsedKafkaWorker = new KafkaWorker("GUIDMONITOR", evn, "ParsedDataCaptureComponents");
		parsedKafkaWorker.workWithWorker(record -> {
			try {
				parsedCount[0]++;
				String guid = record.value().get("guid").asText();
				if (guid.equalsIgnoreCase("65576b469e8246ae3322c30d")) {
					ps.println("Parsed " + record.value().toString());
				}
				if(guidSet.contains(guid)){
					guidSet.remove(guid);
					ps.println(guidSet.size() + " size Parsed set" );
				}
				if (record.offset() % 10000 == 0) {
//					ps.println(guidSet.size() + " size Parsed set" );

					Long timestamp = record.value().get("hittimestamp").asLong();
					ps.println(
							parsedCount[0] + " PV parsed timestamp " + timestamp + " diff " + (System.currentTimeMillis() - timestamp));
				}
				return false;
			}catch (Exception e)
			{
				return false;
			}
		});
	}
}
