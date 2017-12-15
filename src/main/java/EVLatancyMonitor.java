/**
 * Created by pawnow on 7/17/17.
 */
public class EVLatancyMonitor {
	public static void main(String[] args){
		KafkaWorker rawKafkaWorker = new KafkaWorker("EVLatancyMonitorRaw", KafkaWorker.Evn.PROD, "DataCaptureEvents");
		new Thread(() -> {
			rawKafkaWorker.workWithWorker(stringJsonNodeConsumerRecord -> {
				if(stringJsonNodeConsumerRecord.offset()%100==0){
					Long timestamp = stringJsonNodeConsumerRecord.value().get("timestamp").asLong();
					System.out.println("EV raw timestamp "  +timestamp + " diff "+ (System.currentTimeMillis()-timestamp));
				}
				return false;
			});
		}).start();
		KafkaWorker parsedKafkaWorker = new KafkaWorker("EVLatancyMonitorParsed", KafkaWorker.Evn.PROD, "ParsedDataCaptureEvents");
		parsedKafkaWorker.workWithWorker(stringJsonNodeConsumerRecord -> {
			if(stringJsonNodeConsumerRecord.offset()%1000==0){
				Long timestamp = stringJsonNodeConsumerRecord.value().get("timestamp").asLong();
				System.out.println("EV parsed timestamp "  +timestamp + " diff "+ (System.currentTimeMillis()-timestamp));
			}
			return false;
		});
	}
}
