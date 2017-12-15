import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.stream.Collectors;

///**
// * Created by pawnow on 7/11/17.
// */
public class DataGenerator {
    public static void main(String[] args) throws InterruptedException {
        Properties configProperties = new Properties();

                configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        "localhost:9092");
        //		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "testflow-kafka04.gazeta.pl:6667,testflow-kafka02.gazeta.pl:6667");
        //		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dataflow-kafka01.gazeta.pl:6667,dataflow-kafka03.gazeta.pl:6667");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "DataGenerator");
        //              configProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple"+ UUID.randomUUID().toString());
//        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProperties.put("security.protocol", "PLAINTEXT");

//        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(configProperties);
//        while(true) {
//            for(int i=0;i<10;i++){
//                kafkaProducer.send(new ProducerRecord<String, String>("topic1", UUID.randomUUID().toString(), System.currentTimeMillis()+"bleble"));
//            }
//            Thread.sleep(10);
//        }

        KafkaConsumer<String,String> kc =  new KafkaConsumer<String,String>(configProperties);
        List<PartitionInfo> partitionInfoList = kc.partitionsFor("topic");
        List<TopicPartition> topicPartitionList = partitionInfoList.stream().map(partitionInfo -> new TopicPartition(partitionInfo
                .topic(), partitionInfo.partition())).collect(Collectors.toList());
        Map<TopicPartition,Long> endOffsets =  kc.endOffsets(topicPartitionList);
        Map<TopicPartition,OffsetAndMetadata> map = new HashMap<>();
        topicPartitionList.forEach(topicPartition -> map.put(topicPartition,new OffsetAndMetadata(endOffsets.get(topicPartition))));
        kc.commitSync(map);
    }
}
