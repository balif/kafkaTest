/**
 * Created by pawnow on 7/17/17.
 */
public class PVLatancyMonitor {
	public static void main(String[] args){
		KafkaWorker rawKafkaWorker = new KafkaWorker("PVLatancyMonitorRaw", KafkaWorker.Evn.PROD, "DataCaptureComponents");
		new Thread(() -> {
			rawKafkaWorker.workWithWorker(stringJsonNodeConsumerRecord -> {
				if(stringJsonNodeConsumerRecord.offset()%100==0){
					Long timestamp = stringJsonNodeConsumerRecord.value().get("hitTimestamp").asLong();
					System.out.println("PV raw timestamp "  +timestamp + " diff "+ (System.currentTimeMillis()-timestamp));
				}
				return false;
			});
		}).start();
//		KafkaWorker parsedKafkaWorker = new KafkaWorker("PVLatancyMonitorParsed", KafkaWorker.Evn.TEST, "ParsedDataCaptureComponents");
//		parsedKafkaWorker.workWithWorker(stringJsonNodeConsumerRecord -> {
//			if(stringJsonNodeConsumerRecord.offset()%100==0){
//				Long timestamp = stringJsonNodeConsumerRecord.value().get("hittimestamp").asLong();
//				System.out.println("PV parsed timestamp "  +timestamp + " diff "+ (System.currentTimeMillis()-timestamp));
//			}
//			return false;
//		});
	}
}
