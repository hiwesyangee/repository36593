package KafkaAmdHdfs;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import Utils.SparkUtils;
import scala.Tuple2;

public class KafkaConsumer extends Thread implements Serializable {
	private static final long serialVersionUID = 1L;
	private String topic;

	public KafkaConsumer(String topic) {
		this.topic = topic;
	}

	@Override
	public void run() {
		// 初始化Spark连接
		JavaSparkContext sc = SparkUtils.getInstance().sc;
		sc.setLogLevel("ERROR");

		// 定义sparkstreaming流对象
		JavaStreamingContext jssc = SparkUtils.getInstance().jssc;

		Map<String, Object> kafkaParams = new HashMap<>(); // 建立kafka连接
		kafkaParams.put("metadata.broker.list", KafkaProperties.BROKER_LIST);
		kafkaParams.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
		kafkaParams.put("group.id", KafkaProperties.GROUP_ID);
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false); // 避免出现重复数据和数据丢失，false自动提交偏移量

		Collection<String> topics = Arrays.asList(topic);
		// 接收Kafka实时的用户行为数据
		JavaInputDStream<ConsumerRecord<String, String>> lines = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		// Java8风格代码，对JavaInputDStream<ConsumerRecord<Object, Object>>进行操作，得到JavaDStream<Rating>
		JavaPairDStream<String, String> words = lines
				.mapToPair(x -> new Tuple2<String, String>(String.valueOf(x.key()), String.valueOf(x.value())));

		JavaDStream<String> word = words.flatMap(x -> Arrays.asList(x._2).iterator());

		JavaDStream<Rating> kafkaRatings = word.map(s -> new Rating(Integer.parseInt(s.split(",")[0]),
				Integer.parseInt(s.split(",")[1]), Double.parseDouble(s.split(",")[2])));

		System.out.println("Message received");
		// 打印接收到的前十评分信息
		kafkaRatings.print();
		JavaDStream<String> map = kafkaRatings.map(new Function<Rating, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Rating r) throws Exception {
				String user = String.valueOf(r.user());
				String product = String.valueOf(r.product());
				String rating = String.valueOf(r.rating());
				return new String(user+","+product+","+rating);
			}
		});
		
		//将每一个DStream进行遍历，将数据存入hdfs
		map.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				String path = "hdfs://hy:8020/opt/testData/streamingData" + String.valueOf(new Date().getTime());
				rdd.saveAsTextFile(path);
			}
		});
		
		try {
			jssc.start();
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
