import com.google.common.collect.Sets;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.PrintStream;
import java.util.Set;

/**
 * Created by pawnow on 7/17/17.
 */
public class KafkaTopicView {
	public static void main(String[] args) {

		KafkaWorker.Evn evn = KafkaWorker.Evn.TEST;

//
//		KafkaWorkerAutoRebalance rawKafkaWorker = new KafkaWorkerAutoRebalance("MONITOR", evn, "test");
//		PrintStream ps = System.out;
//		long[] rawCount = new long[]{0};
//		long[] parsedCount = new long[]{0};
//		Set<String> guidSet = Sets.newConcurrentHashSet();
//		System.out.println("test");
//		for(int i=0 ; i<10 ;i++) {
//			final String treadName = "t"+i+ " ";
//			new Thread(() -> {
//				rawKafkaWorker.workWithWorker(record -> {
////				System.out.println("mess");
//					System.out.println(treadName + record.value());
//					return false;
//				}, 0L);
//			}).start();
//		}

		KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(KafkaWorkerString.getProperties("GENERATESTRING", KafkaWorker.Evn.TEST));
		for (int i = 1; i < 110; i++){
			System.out.println("send");
			kafkaProducer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)+"abcd"));
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
