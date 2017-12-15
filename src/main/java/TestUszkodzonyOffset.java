import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

///**
// * Created by pawnow on 7/11/17.
// */
public class TestUszkodzonyOffset {
    private static final String TOPIC ="ParsedDataCaptureComponents" ;
    private static final long OFFSET = 845802306L;
    private static final int DIFF = 100;

    public static void main(String[] args) {
        Properties configProperties = KafkaWorkerString.getProperties("testUszkodzonyOffser", KafkaWorker.Evn.PROD);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
        try {

//			List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(TOPIC);
//			List<TopicPartition> topicPartitionList = partitionInfoList.stream().map(partitionInfo -> new TopicPartition(partitionInfo
//					.topic(), partitionInfo.partition())).collect(Collectors.toList());
//			Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
//			topicPartitionList.forEach(topicPartition -> topicPartitionLongMap.put(topicPartition, startTimestamp));
//			Set<Map.Entry<TopicPartition, OffsetAndTimestamp>> set = kafkaConsumer.offsetsForTimes(topicPartitionLongMap).entrySet();
//			kafkaConsumer.assign(topicPartitionList);
            kafkaConsumer.subscribe(Lists.asList(TOPIC, new String[0]), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("onPartitionsRevoked");
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("onPartitionsAssigned");
                    partitions.forEach(topicPartition -> kafkaConsumer.seek(topicPartition, OFFSET- DIFF) );
                }
            });
//			topicPartitionList.forEach(topicPartition -> kafkaConsumer.seek(topicPartition, 0));
//			set.forEach(entry -> kafkaConsumer.seek(entry.getKey(), entry.getValue().offset()));
            int i =0;
            Map<Integer,Integer> j = new HashMap<>();
            stop:
            while (true) {
                try {
                    i++;
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        Integer jV = j.getOrDefault(record.partition(),0);

                        if(jV<DIFF*2){
                            j.put(record.partition(),++jV);
                            if(record.offset()>OFFSET-3 && record.offset()<OFFSET+3){
                                System.out.print("-------");
                            }
                            System.out.println("i"+i + " j"+jV + " partition" +record.partition() + " offset" + record.offset() + " value" + record.value());
                        }
                        if(record.offset()>OFFSET+DIFF && record.partition()==1)break stop;

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            j.forEach((k, v) -> System.out.println("Partition " + k + " v " + v));
        } finally {
            kafkaConsumer.close();

        }
    }

//	public static void main(String[] args) throws InterruptedException, IOException {
//		ClientCnf clientCnf = new ClientCnf();
//		clientCnf.setServers(Arrays.asList(new String[]{"ocean-metrics01.gazeta.pl"}));
//		Long ttlInSec = 2000L;
//		clientCnf.setTTL(ttlInSec.intValue());
//		ClientPV clientPV = new ClientPV(clientCnf,"parsedDataCache_PROD");
//		int i = 0 ;
//
//		File folder = new File("/home/pawnow/Documents/pvJoinerTest");
//		Connection connection;
//		CallableStatement callableStatement;
//		try {
//
//			connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/pawnow", "pawnow", "1234");
//			callableStatement = connection.prepareCall("Select * from pvstats where pvid in (?)");
//
//		} catch (SQLException e) {
//
//			System.out.println("Connection Failed! Check output console");
//			e.printStackTrace();
////			return;
//		}
//
////		for(File f : folder.listFiles()) {
//			//		while(i<1) {
//			try {
////				FileReader fr = new FileReader(f);
////				BufferedReader br = new BufferedReader(fr);
//				Set<String> set = new HashSet<String>();
////				set = br.lines().flatMap(s -> Stream.of(s.split(","))).map(s -> s.replaceAll("'", "")).collect(Collectors.toSet());
//							clientPV.pushPV(Arrays.asList(new PV[] { new PV("12345", "test") }));
//							set.add("fe55ba47feb1353fa4ee14f4");
//							set.add("88f2774c3d9b8affc7a0c19b");
//							set.add("26dfb84d85b7ed0899ceef27");
//							set.add("89575f4a71bb04d12c21a644");
//							set.add("6f40ce402bba74aa292d3659");
//							set.add("09ba3e4d3d878951508b8f7c");
//							set.add("ef5bba4efc9da6ad7e701bf9");
//
////				System.out.println("---- "+ f.getName() +" ------");
//				Map<String, PV> map = clientPV.getPv(set);
//				map.forEach((p1, p2) -> {
//					System.out.println(p1 + " " + p2.getJsonData());
//				});
//
////				callableStatement.setString(1,set.stream().reduce((s, s2) -> s+","+s2).get());
////				ResultSet resultSet = callableStatement.executeQuery();
//				Thread.sleep(1000);
////				br.close();
//			} catch (IllegalStateException e) {
//				System.out.println(e.getMessage());
//				clientPV.close();
//
//				try {
//					clientPV = new ClientPV(clientCnf,"parsedDataCache_PROD");
//				} catch (Exception ex) {
//					System.out.println(e.getMessage());
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
////		}
////		}
//		System.exit(0);
//
//	}
}
