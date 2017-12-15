import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EVAnalysis {
    private static final int miliSecToHourFactor = 1000 * 60 * 60;
    private static Scanner in;
      private static boolean stop = false;
    private static List<ConsumerThread> thredList= new LinkedList<>();

    public static void main(String[] argv)throws Exception{

          String topicName = "DataCaptureEvents";
          String groupId = "PVJoinTest";
          KafkaConsumer<String,JsonNode> kafkaConsumer;

          Properties configProperties = getProperties(groupId);





          //Figure out where to start processing messages from
          kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
          //              kafkaConsumer.offsetsForTimes()

          List<PartitionInfo> partInfo = kafkaConsumer.partitionsFor(topicName);
          List<TopicPartition> tplist= Arrays.asList(new TopicPartition(topicName,0));
          Map<TopicPartition,Long> endMap = kafkaConsumer.endOffsets(tplist);
          Map<TopicPartition,Long> beginningOffsetsMap = kafkaConsumer.beginningOffsets(tplist);
          Long endOffset[] =  new Long[]{0L};
          Long startOffset[] =  new Long[]{Long.MAX_VALUE};
          endMap.forEach((topicPartition, aLong) -> {if(endOffset[0]<aLong) endOffset[0]=aLong;});
          beginningOffsetsMap.forEach((topicPartition, aLong) -> {if(startOffset[0]>aLong) startOffset[0]=aLong;});

          long difOffsets=endOffset[0]-startOffset[0];
          difOffsets=difOffsets/2;
          int thredCount =10;
          long thredRangeSize = difOffsets/thredCount;
          Map[] arrayMap = new Map[thredCount];

        ExecutorService es = Executors.newFixedThreadPool(thredCount/2);
        for(int i =0; i<thredCount;i++){
            long offsetTStart=startOffset[0]+thredRangeSize*i;
            long offsetTEnd=startOffset[0]+thredRangeSize*(i+1);
            String thredGroupId=groupId+i;
            arrayMap[i]=new HashMap<>();

            es.execute(new ConsumerThread(topicName, groupId, offsetTStart, offsetTEnd, arrayMap[i]));
        }
        es.shutdown();
          es.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
          Map<String,MinMax> mapRes = arrayMap[0];
          for(int i =1; i<arrayMap.length;i++){
              Map<String,MinMax> mapIn = arrayMap[i];
              mapIn.forEach((s, minMax) -> {
                  if(mapRes.containsKey(s)){
                      mapRes.get(s).join(minMax);
                  }else{
                      mapRes.put(s,minMax);
                  }

              });
          }

          Map<Long,Long> res = mapRes.values().stream().map(s -> s.max-s.min).map(aLong -> aLong/ miliSecToHourFactor).collect(Collectors.groupingByConcurrent(
                  Function.identity(),Collectors.counting()));

          PrintWriter writer = new PrintWriter("/home/pawnow/Documents/evCalcres.txt", "UTF-8");
          res.entrySet().forEach(s->writer.println(s.getKey()+" -> " + s.getValue()));
        writer.close();
          kafkaConsumer.close();

      }

    private static Properties getProperties(String groupId) {
        Properties configProperties = new Properties();
        //              configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "testflow-kafka04.gazeta.pl:6667,testflow-kafka02.gazeta.pl:6667");
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dataflow-kafka01.gazeta.pl:6667,dataflow-kafka03.gazeta.pl:6667");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //              configProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
        configProperties.put("security.protocol", "SASL_PLAINTEXT");
        configProperties.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" +
				"\t\t\t\t\tuseKeyTab=true\n" + "\t\t\t\t\tkeyTab=\"/etc/security/keytabs/prod-dataocean.keytab\"\n" +
				"\t\t\t\t\tstoreKey=true\n" + "\t\t\t\t\tuseTicketCache=false\n" + "\t\t\t\t\tserviceName=\"kafka\"\n" +
				"\t\t\t\t\tprincipal=\"prod-dataocean@AGORA.PL\";");


        //              configProperties.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" +
        //                      "\t\t\t\t\tuseKeyTab=true\n" + "\t\t\t\t\tkeyTab=\"/etc/security/keytabs/test-testocean.keytab\"\n" +
        //                      "\t\t\t\t\tstoreKey=true\n" + "\t\t\t\t\tuseTicketCache=false\n" + "\t\t\t\t\tserviceName=\"kafka\"\n" +
        //                      "\t\t\t\t\tprincipal=\"test-testocean@AGORA.PL\";");

        return configProperties;
    }

    private static class ConsumerThread extends Thread{
          private String topicName;
          private String groupId;
          private KafkaConsumer<String,JsonNode> kafkaConsumer;
        private long offset;
        private long offsetStop;
        private Map<String,MinMax> map;

        public ConsumerThread(String topicName, String groupId, long offset, long offsetStop,Map<String,MinMax> map){
              this.topicName = topicName;
              this.groupId = groupId;
                this.offset = offset;
            this.offsetStop = offsetStop;
            this.map=map;
        }
          public void run() {
              Properties configProperties = getProperties(groupId);


//              configProperties.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required\n" +
//                      "\t\t\t\t\tuseKeyTab=true\n" + "\t\t\t\t\tkeyTab=\"/etc/security/keytabs/test-testocean.keytab\"\n" +
//                      "\t\t\t\t\tstoreKey=true\n" + "\t\t\t\t\tuseTicketCache=false\n" + "\t\t\t\t\tserviceName=\"kafka\"\n" +
//                      "\t\t\t\t\tprincipal=\"test-testocean@AGORA.PL\";");


              //Figure out where to start processing messages from
              kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
//              kafkaConsumer.offsetsForTimes()
              List<TopicPartition> tplist= Arrays.asList(new TopicPartition(topicName,0));
              kafkaConsumer.assign(tplist);
              kafkaConsumer.seek(tplist.get(0),offset);
              //Start processing messages
              try {
                  endLoop:
                  while (true) {
                      ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(100);
                      for (ConsumerRecord<String, JsonNode> record : records) {
                          JsonNode context = record.value().get("context");
                          if(context==null || context.isNull()) continue;
                          JsonNode pvidNode = context.get("pageviewid");
                          if(pvidNode==null || pvidNode.isNull()) continue;

                          String pvid = pvidNode.asText();
                          long timestamp = record.value().get("timestamp").asLong();
                          if(map.containsKey(pvid)){
                              MinMax mm = map.get(pvid);
                              if(mm.min>timestamp){
                                  mm.min=timestamp;
                              }
                              if(mm.max<timestamp){
                                  mm.max=timestamp;
                              }
                          }else{
                              map.put(pvid,new MinMax(timestamp,timestamp));
                          }
                          if(record.offset()>=offsetStop) {
                              break endLoop;
                          }else{
                              if(record.offset()%10000==0) {
                                  int perc = Double.valueOf(((double)((record.offset()-offset))/(double)((offsetStop-offset)))*100).intValue();
                                  System.out.println( Thread.currentThread().getName() + " " + perc+"%");
                              }
                          }
                      }

                  }
//                  map.forEach((s, minMax) -> System.out.println(s + " " + " min: " + minMax.min + " max: " + minMax.max));
                  List<Long> res = map.values().stream().map(s -> s.max-s.min).sorted().collect(Collectors.toList());

//                  res.forEach(s->System.out.println(s));
//                  File f = new File("/home/pawnow/Documents/evCalcres.txt");
                  PrintWriter writer = new PrintWriter("/home/pawnow/Documents/evCalcres_"+groupId+".txt", "UTF-8");
                  res.forEach(s->writer.println(s));
                  writer.close();
              }catch(WakeupException ex){
                  System.out.println("Exception caught " + ex.getMessage());
              } catch (FileNotFoundException e) {
                  e.printStackTrace();
              } catch (UnsupportedEncodingException e) {
                  e.printStackTrace();
              } finally{
                  kafkaConsumer.close();
                  System.out.println("After closing KafkaConsumer");
              }
          }
          public KafkaConsumer<String,JsonNode> getKafkaConsumer(){
             return this.kafkaConsumer;
          }
      }

    private static class MinMax {

          long min=0;
          long max=0;

        public MinMax(long min, long max) {
            this.min = min;
            this.max = max;
        }

        void join(MinMax minMax){
            if(minMax.min<min){
                min=minMax.min;
            }
            if(minMax.max>max){
                max=minMax.max;
            }
        }
    }
}
