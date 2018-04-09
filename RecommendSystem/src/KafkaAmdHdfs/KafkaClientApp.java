package KafkaAmdHdfs;

/**
 * @ClassName: KafkaClientApp
 * @Description: Kafka Java API测试
 * @author: hiwes
 * @date: 2018年4月2日
 */
public class KafkaClientApp {
	public static void main(String[] args) {
		new KafkaProducer(KafkaProperties.TOPIC).start();
		new KafkaConsumer(KafkaProperties.TOPIC).start();
	}
}