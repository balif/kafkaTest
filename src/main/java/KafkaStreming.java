import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreming {

    public static void main(final String[] args) throws Exception {
        Properties config = KafkaWorkerString.getProperties("streamGrId", KafkaWorker.Evn.TEST);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-test-application1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "testflow-kafka04.gazeta.pl:6667,testflow-kafka02.gazeta.pl:6667");
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());

//        config.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, )


        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream("DataCaptureComponents");

//        KTable<String, Long> wordCounts = textLines
////                .groupBy((key, word) -> "pvGroup")
////                .count("Counts");
//        .map((key, value) -> new KeyValue<>("pv",1L)).;
//        wordCounts.to(Serdes.String(), Serdes.Long(), "testCountTopic");
//        textLines.map((key, value) -> new KeyValue<>("pv","1")).groupBy((key, word) -> "pvGroup").count().to(Serdes.String(), Serdes.Long(),"testCountTopic");


                KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
        Thread.sleep(500000L);
        streams.close();
    }
}
