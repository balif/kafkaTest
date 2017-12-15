//import com.fasterxml.jackson.databind.JsonNode;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.TopicPartition;
//import pl.agora.bigdata.parser.api.model.PV;
//import pl.agora.bigdata.parser.client.ClientPV;
//import pl.agora.bigdata.parser.client.cnf.ClientCnf;
//
//import java.util.*;
//
///**
// * Created by pawnow on 7/13/17.
// */
public class PostCheckHits {
//
//	public static void main(String[] argv) throws Exception {
//		String topicName = "ParsedDataCaptureEvents";
//		String groupId = "PVJoinTest";
//
//		KafkaConsumer<String, JsonNode> kafkaConsumer = null;
//		try {
//
//			Properties configProperties = getProperties(groupId);
//
//			//Figure out where to start processing messages from
//			kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
//			//              kafkaConsumer.offsetsForTimes()
//
//			List<TopicPartition> tplist= Arrays.asList(new TopicPartition(topicName,0));
//			Map<TopicPartition,Long> endMap = kafkaConsumer.endOffsets(tplist);
//			Long endOffset[] =  new Long[]{0L};
//			endMap.forEach((topicPartition, aLong) -> {if(endOffset[0]<aLong) endOffset[0]=aLong;});
//
//			kafkaConsumer.assign(tplist);
//			kafkaConsumer.seekToBeginning(tplist);
//			Long start = kafkaConsumer.position(tplist.get(0));
//			Set<String> idList = new HashSet<>();
//			stop:
//			while (true) {
//				ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(100);
//				for (ConsumerRecord<String, JsonNode> record : records) {
//					JsonNode context = record.value().get("context");
//					if (context == null || context.isNull())
//						continue;
//					JsonNode pvidNode = context.get("pageviewid");
//
//					if (pvidNode == null || pvidNode.isNull() || pvidNode.asText().isEmpty()) {
//					} else {
//						JsonNode pvData = record.value().get("pvData");
//
//						if (pvData == null || pvData.isNull()) {
//							idList.add(pvidNode.asText());
//						} else {
//						}
//					}
//					if(record.offset()>=endOffset[0]) {
//						break stop;
//					}else{
//						if(record.offset()%10000==0) {
//
//							int perc = Double.valueOf(((double)((record.offset()-start))/(double)((endOffset[0]-start)))*100).intValue();
//							System.out.println( Thread.currentThread().getName() + " " + perc+"%" + " " +record.offset());
//						}
//					}
//				}
//
//			}
//
//			ClientCnf clientCnf = new ClientCnf();
//			clientCnf.setServers(Arrays.asList(new String[]{"bigdata-smart.gazeta.pl"}));
//			Long ttlInSec = 2000L;
//			clientCnf.setTTL(ttlInSec.intValue());
//			ClientPV clientPV = new ClientPV(clientCnf,"parsedDataCache_PROD");
//			Map<String,PV> resMap = clientPV.getPv(idList);
//			System.out.println(Double.valueOf(resMap.size())/idList.size());
//		} finally {
//			kafkaConsumer.close();
//
//		}
//
//	}
//
//	private static Properties getProperties(String groupId) {
//		Properties configProperties = new Properties();
//		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "testflow-kafka04.gazeta.pl:6667,testflow-kafka02.gazeta.pl:6667");
//		//		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dataflow-kafka01.gazeta.pl:6667,dataflow-kafka03.gazeta.pl:6667");
//		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
//		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//		//              configProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
//		configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
//		configProperties.put("security.protocol", "SASL_PLAINTEXT");
//		//		configProperties.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" +
//		//				"\t\t\t\t\tuseKeyTab=true\n" + "\t\t\t\t\tkeyTab=\"/etc/security/keytabs/prod-dataocean.keytab\"\n" +
//		//				"\t\t\t\t\tstoreKey=true\n" + "\t\t\t\t\tuseTicketCache=false\n" + "\t\t\t\t\tserviceName=\"kafka\"\n" +
//		//				"\t\t\t\t\tprincipal=\"prod-dataocean@AGORA.PL\";");
//
//		configProperties.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" + "\t\t\t\t\tuseKeyTab=true\n" +
//				"\t\t\t\t\tkeyTab=\"/etc/security/keytabs/test-testocean.keytab\"\n" + "\t\t\t\t\tstoreKey=true\n" +
//				"\t\t\t\t\tuseTicketCache=false\n" + "\t\t\t\t\tserviceName=\"kafka\"\n" +
//				"\t\t\t\t\tprincipal=\"test-testocean@AGORA.PL\";");
//
//		return configProperties;
//	}
}
