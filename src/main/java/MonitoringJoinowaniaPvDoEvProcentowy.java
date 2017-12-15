import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by pawnow on 7/13/17.
 */
public class MonitoringJoinowaniaPvDoEvProcentowy {

	public static void main(String[] argv) throws Exception {
		final long[] okCounter = { 0 };
		final long[] falseCounter = { 0 };
		final long[] totalCounter = { 0 };
		final long[] dealayedCounter = { 0 };
		String topicName = "ParsedDataCaptureEventsWithPv";
		String groupId = "PVJoinTestNewNew";
		final File[] file = { null };
		final BufferedWriter[] bufferedWriter = { null };

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm");
		KafkaConsumer<String, JsonNode> kafkaConsumer = null;
		try {

			Properties configProperties = getProperties(groupId);

			//Figure out where to start processing messages from
			kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
			//              kafkaConsumer.offsetsForTimes()
			List<TopicPartition> tplist= Arrays.asList(new TopicPartition(topicName,0));
			kafkaConsumer.assign(tplist);
			kafkaConsumer.seekToEnd(tplist);
			KafkaWorker kafkaWorker = new KafkaWorker(groupId, KafkaWorker.Evn.PROD,topicName);
			final long[] okTimestamp = { 0 };
			final long[] failTimestamp = { 0 };
			final long[] delayTimestamp = { 0 };
			kafkaWorker.workWithWorker(record -> {

					totalCounter[0]++;
					JsonNode context = record.value().get("context");
					if (context == null || context.isNull())
						return false;
					JsonNode pvidNode = context.get("pageviewid");

					if (pvidNode == null || pvidNode.isNull() || pvidNode.asText().isEmpty()) {
						okCounter[0]++;
						okTimestamp[0] +=System.currentTimeMillis()-record.value().get("timestamp").asLong();
					} else {
						JsonNode pvData = record.value().get("pvData");

						if (pvData == null || pvData.isNull()) {
							Date date = new Date();
							String fileName = simpleDateFormat.format(date)+".txt";
							if(file[0] ==null || !file[0].getName().equals(fileName)){
								file[0] =new File("/home/pawnow/Documents/pvJoinerTest/"+fileName);
								try {
									file[0].createNewFile();
								} catch (IOException e) {
									e.printStackTrace();
								}
								if(bufferedWriter[0]!=null){
									try {
										bufferedWriter[0].close();
									} catch (IOException e) {
										e.printStackTrace();
									}
								}
								try {
									FileWriter fw = new FileWriter(file[0]);
									bufferedWriter[0] = new BufferedWriter(fw);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}

							falseCounter[0]++;
							failTimestamp[0] +=System.currentTimeMillis()-record.value().get("timestamp").asLong();
							try {
								bufferedWriter[0].append("'"+pvidNode.asText()+"'"+",");
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else {
							JsonNode tags = record.value().get("tags");
							if(tags!=null && !tags.isNull()){
								dealayedCounter[0]++;
								delayTimestamp[0] +=System.currentTimeMillis()-record.value().get("timestamp").asLong();
							}
							okCounter[0]++;
							okTimestamp[0] +=System.currentTimeMillis()-record.value().get("timestamp").asLong();
						}
					}
					//					System.out.println(record.offset());
					if (record.offset() % 1000 == 0) {
						//						long currenttimestamp = System.currentTimeMillis();

						double procent = ((Double.valueOf(okCounter[0]) / Double.valueOf(totalCounter[0])) * 100.0);
						double delayProcent = ((Double.valueOf(dealayedCounter[0]) / Double.valueOf(okCounter[0])) * 100.0);
						System.out.println("okCounter: " + okCounter[0] + " dealayedCounter: " + dealayedCounter[0] + " falseCounter: " +
								falseCounter[0] + " totalCounter: " + totalCounter[0] +
								" procent: " + procent + "%"  +
								" delayProcent: " + delayProcent + "%");
						long diffOk= okCounter[0] ==0?0: okTimestamp[0] / okCounter[0];
						long diffFail= falseCounter[0] ==0?0: failTimestamp[0] / falseCounter[0];
						long diffDelay= dealayedCounter[0] ==0?0: delayTimestamp[0] / dealayedCounter[0];
						System.out.println("diffOk: " + diffOk + " diffFail: " + diffFail + " fail-ok: " + (diffFail-diffOk) + " diffDelay " + diffDelay + " diffDelay-ok: " + (diffDelay-diffOk)) ;
						System.out.println("diffOk: " + diffOk/1000 + " diffFail: " + diffFail/1000 + " fail-ok: " + ((diffFail-diffOk)/1000)+ " diffDelay " + (diffDelay/1000));


					}
					if(record.offset() % 100000 == 0){
						okCounter[0] = 0;
						falseCounter[0] = 0;
						totalCounter[0] = 0;
						okTimestamp[0] =0;
						dealayedCounter[0] =0;
						failTimestamp[0] =0;
						delayTimestamp[0] =0;
					}
					return false;

			});

//			kafkaConsumer.subscribe(Arrays.asList(topicName));

			while (true) {

			}
		} finally {
			kafkaConsumer.close();

		}

	}

	private static Properties getProperties(String groupId) {
		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "testflow-kafka04.gazeta.pl:6667,testflow-kafka02.gazeta.pl:6667");
		//		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dataflow-kafka01.gazeta.pl:6667,dataflow-kafka03.gazeta.pl:6667");
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		//              configProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
		configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
		configProperties.put("security.protocol", "SASL_PLAINTEXT");
		//		configProperties.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" +
		//				"\t\t\t\t\tuseKeyTab=true\n" + "\t\t\t\t\tkeyTab=\"/etc/security/keytabs/prod-dataocean.keytab\"\n" +
		//				"\t\t\t\t\tstoreKey=true\n" + "\t\t\t\t\tuseTicketCache=false\n" + "\t\t\t\t\tserviceName=\"kafka\"\n" +
		//				"\t\t\t\t\tprincipal=\"prod-dataocean@AGORA.PL\";");

		configProperties.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" + "\t\t\t\t\tuseKeyTab=true\n" +
				"\t\t\t\t\tkeyTab=\"/etc/security/keytabs/test-testocean.keytab\"\n" + "\t\t\t\t\tstoreKey=true\n" +
				"\t\t\t\t\tuseTicketCache=false\n" + "\t\t\t\t\tserviceName=\"kafka\"\n" +
				"\t\t\t\t\tprincipal=\"test-testocean@AGORA.PL\";");

		return configProperties;
	}
}
