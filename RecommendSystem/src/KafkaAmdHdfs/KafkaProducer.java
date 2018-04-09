package KafkaAmdHdfs;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

/**
 * @ClassName: KafkaProducer 
 * @Description: Kafka生产者
 * @author: hiwes
 * @date: 2018年4月2日
 */
public class KafkaProducer extends Thread{
    private String topic;
    private Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();

        properties.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");

        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        while(true) {
        	Random random = new Random();
        	int userId = random.nextInt(1000)%(1000) + 1;			
			int bookId = random.nextInt(10000)%(10000) + 1;		
			String[] doc = { "1.0", "2.0", "3.0", "4.0", "5.0" };
			int index = (int) (Math.random() * doc.length);
			String rating = doc[index];
            String message = String.valueOf(userId) + "," + String.valueOf(bookId) + "," + rating;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Sent: " + message);
            try{
                Thread.sleep(2000);
            } catch (Exception e){
                e.printStackTrace();
            }
        }

    }
}
