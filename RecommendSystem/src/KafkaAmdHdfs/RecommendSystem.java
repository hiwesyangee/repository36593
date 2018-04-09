package KafkaAmdHdfs;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import Utils.HBaseUtils;
import Utils.SparkUtils;

public class RecommendSystem implements Serializable {
	private static final long serialVersionUID = 1L;
	public static void main(String[] args) throws IOException{
		JavaSparkContext sc = SparkUtils.getInstance().sc;
		//先读取了hdfs上的文件。
		JavaRDD<String> allLines = sc.textFile("hdfs://hy:8020/opt/testData/rating2");
		JavaRDD<Rating> allRatings = allLines.map(new Function<String, Rating>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Rating call(String s) throws Exception {
				int user = Integer.parseInt(s.split(",")[0]);
				int product = Integer.parseInt(s.split(",")[1]);
				double rating = Double.parseDouble(s.split(",")[2]);
				return new Rating(user, product, rating);
			}
		});
		//初始化用户推荐表
		System.out.println("初始化用户推荐表");
		long start = System.currentTimeMillis();
		MatrixFactorizationModel firstModel = ALS.train(allRatings.rdd(), 10, 10, 0.1);
		List<Integer> userId = allRatings.map(r -> Integer.valueOf(r.user())).distinct().collect();
		for (int user : userId) {
			Rating[] topRatings = firstModel.recommendProducts(user, 10); // 得到推荐的结果
			String books = "";
			String ratings = "";
			// 将推荐结果直接存入HBase表——ratings_recTable。
			for (Rating r : topRatings) {
				books += String.valueOf(r.product()) +",";
				ratings += String.valueOf(r.rating()) + ",";
			}
			String data1 = books.substring(0, books.length() - 1);
			String data2 = ratings.substring(0, ratings.length() - 1);
			HBaseUtils.getInstance().put("test", String.valueOf(user), "cf", "recBooks", data1);
			HBaseUtils.getInstance().put("test", String.valueOf(user), "cf", "recRatings", data2);
		}
		
		long stop = System.currentTimeMillis();
		// 再准备流式读取文件
		System.out.println("初始化结束，用时"+ (stop-start)*1.0 + "  ms  " +",等待接收文件");
		JavaStreamingContext jssc = SparkUtils.getInstance().jssc;

		JavaDStream<String> lines = jssc.textFileStream("hdfs://hy:8020/opt/testData/");
		JavaDStream<Rating> hdfsRatings = lines.map(new Function<String, Rating>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Rating call(String s) throws Exception {
				int user = Integer.parseInt(s.split(",")[0]);
				int product = Integer.parseInt(s.split(",")[1]);
				double rating = Double.parseDouble(s.split(",")[2]);
				return new Rating(user, product, rating);
			}
		});
		System.out.println("foreach");
		//在流中进行rdd的合并，将合并内容进行存储。
		hdfsRatings.foreachRDD(new VoidFunction<JavaRDD<Rating>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<Rating> rdd) throws Exception {
				List<Integer> userId = rdd.map(r -> Integer.valueOf(r.user())).distinct().collect();
				JavaRDD<Rating> union = allRatings.union(rdd).cache();
				//将合并之后的RDD进行存储
				JavaRDD<String> result = union.map(new Function<Rating, String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public String call(Rating r) throws Exception {
						String user = String.valueOf(r.user());
						String product = String.valueOf(r.product());
						String rating = String.valueOf(r.rating());
						return new String(user+","+product+","+rating);
					}
				});
				result.saveAsTextFile("hdfs://hy:8020/opt/testData/rating");
				//使用合并后的rdd进行建模，
				MatrixFactorizationModel model = ALS.train(union.rdd(), 10, 10, 0.1);
				//准备更新只是有行为数据的用户数据
				rdd.cache();
				for (int user : userId) {
					System.out.println(String.valueOf(user));
					Rating[] topRatings = model.recommendProducts(user, 10);            // 得到推荐的结果
					String books = "";
					String ratings = "";
					// 将推荐结果直接存入HBase表——ratings_recTable。
					for (Rating r : topRatings) {
						books += String.valueOf(r.product()) +",";
						ratings += String.valueOf(r.rating()) + ",";
					}
					String data1 = books.substring(0, books.length() - 1);
					String data2 = ratings.substring(0, ratings.length() - 1);
					HBaseUtils.getInstance().put("test", String.valueOf(user), "cf", "recBooks", data1);
					HBaseUtils.getInstance().put("test", String.valueOf(user), "cf", "recRatings", data2);
				}
				System.out.println("over!");
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
