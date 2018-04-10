package com.sinyee.cores.recsystem;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.sinyee.cores.recutils.HBaseUtils;

/**
 * @Description 初版推荐系统(正式版)推荐引擎
 * @function 对友乎读书App实时产生的用户数据通过Spring传递到集群目录后，
 *           进行数据分析并将推荐结果存入HBase评分推荐表ratings_recTable。
 * @author hiwes
 * @data 2018年4月10日
 * @version 1.0.0
 */
public class RecommendSystem extends Thread implements Serializable {

	private static final long serialVersionUID = 1L;

	private static SparkConf sparkConf = null;
	private static JavaSparkContext sc = null;
	private static JavaStreamingContext jssc = null;

	public RecommendSystem() {
		sparkConf = new SparkConf();
		sparkConf.setMaster("local");
		sparkConf.setAppName("RecommendSystem");
		// sparkConf.set("spark.executor.memory", "2g");

		sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");
//		sc.addJar("/opt/sinyee.jar");
		
		jssc = new JavaStreamingContext(sc, Durations.seconds(20));

		jssc.checkpoint("hdfs://master:9000/checkpoint");
	}

	static class ParseRating implements Function<String, Rating> {

		private static final long serialVersionUID = 1L;
		private static final Pattern COMMA = Pattern.compile(",");

		@Override
		public Rating call(String s) throws Exception {
			String[] arr = COMMA.split(s);
			int user = Integer.parseInt(arr[0]);
			int product = Integer.parseInt(arr[1]);
			double rating = Double.parseDouble(arr[2]);
			return new Rating(user, product, rating);
		}

	}

	public void run() {
		
		// 初始化推荐信息
		System.out.println("\n"+"初始化信息开始...");
		long start = System.currentTimeMillis();
		JavaRDD<String> lines = sc.textFile("hdfs://master:9000/data/overallData/allRatings.dat");

		JavaRDD<Rating> originalRatings = lines.map(new ParseRating()).cache();
		
		// 获取用户列表
		List<Integer> originalUsers = originalRatings.map(x -> x.user()).distinct().collect();
		int originalUserNumber = originalUsers.size();
		
		MatrixFactorizationModel originalModel = ALS.train(originalRatings.rdd(), 10, 10, 0.1);
		originalRatings.unpersist();
		
		//创建数据库评分推荐表
		String[] cfs = {"rec"};
		try {
			HBaseUtils.getInstance().createTable("ratings_recTable", cfs);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for (int user : originalUsers) {
			Rating[] topRatings = originalModel.recommendProducts(user, 10);
			String recBooks = "";
			String recRatings = "";
			for (Rating r : topRatings) {
				recBooks += String.valueOf(r.product()) + ",";
				recRatings += String.valueOf(r.rating()) + ",";
			}
			String rowKey = String.valueOf(user);
			String bookData = recBooks.substring(0, recBooks.length() - 1);
			String ratingData = recRatings.substring(0, recRatings.length() - 1);
			// 针对用户对HBase表——评分推荐表进行写入
			HBaseUtils.getInstance().putTable("ratings_recTable", rowKey, "rec", "recBooks", bookData);
			HBaseUtils.getInstance().putTable("ratings_recTable", rowKey, "rec", "recRatings", ratingData);
		}
		long stop = System.currentTimeMillis();
		System.out.println("\n"+"初始化数据库完成，写入"+ originalUserNumber +"条数据，共耗时"+(stop-start)*1.0 /1000 +"s");

		// 流式计算，每60s对监控目录进行一次数据流读取
		System.out.println("\n"+"等待接收stream数据");
		JavaDStream<String> stream = jssc.textFileStream("/opt/testData/");
		
		
		JavaDStream<Rating> streamRatings = stream.map(new ParseRating());
		
		//册使用打印：
		streamRatings.print();
		
		streamRatings.foreachRDD(new VoidFunction<JavaRDD<Rating>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Rating> rdd) throws Exception {
//				System.out.println("\n"+"流foreach开始...");
				long start2 = System.currentTimeMillis();
				//获取本次流数据中的用户列表
				List<Integer> streamUsers = rdd.map(x -> x.user()).distinct().collect();
				int streamUserNumber = streamUsers.size();
				
				//读取一次HDFS文件
//				System.out.println("\n"+"——————————1——————————");
				JavaRDD<String> lines = sc.textFile("/opt/data/overallData/allRatings.dat");
				JavaRDD<Rating> allRatings = lines.map(new ParseRating()).cache();
				
				//合并数据
				JavaRDD<Rating> union = allRatings.union(rdd).repartition(10).cache();
				
//				System.out.println("\n"+"——————————2——————————");
				//转化数据并覆盖HDFS文件
				/**
				 * 这一步出现问题：需要将RDD里面的内容转化为.dat文件，并覆盖存入HDFS。
				 * 下面是测试，使用FileOutputStream对hdfs文件进行修改
				 */
				JavaRDD<String> map = rdd.map(x -> new String(String.valueOf(x.user())+","+String.valueOf(x.product())+","+String.valueOf(x.rating())));
				
				List<String> collect = map.collect();
				String data = "";
				for(String c:collect) {
					data += c+"\n";
				}
				Scanner input = new Scanner(data);
				
				FileWriter fos = new FileWriter("/opt/data/overallData/allRatings.dat",true);
				while(input.hasNext()) {
					String aString =input.next();
					fos.write(aString+ "\r\n");
				}
				fos.flush();
				fos.close();
				input.close();
//				System.out.println("\n"+"——————————3——————————");
				
				//选择最佳参数进行建模
//				String[] bestParameter = new BestParameter().getBestParameter(union);
//				int bestNumRank = Integer.parseInt(bestParameter[0]);
//				int bestNumIter = Integer.parseInt(bestParameter[1]);
//				double bestLambda = Double.parseDouble(bestParameter[2]);
				
//				System.out.println("\n"+"——————————4——————————");
				MatrixFactorizationModel bestModel = ALS.train(union.rdd(), 10, 18, 0.1);
				union.unpersist();
				
//				System.out.println("\n"+"——————————5——————————");
				//只对本次流数据中的用户进行数据更新
				for(int user:streamUsers) {
					Rating[] topRatings = bestModel.recommendProducts(user, 10);
					String recBooks = "";
					String recRatings = "";
					for (Rating r : topRatings) {
						recBooks += String.valueOf(r.product()) + ",";
						recRatings += String.valueOf(r.rating()) + ",";
					}
					String rowKey = String.valueOf(user);
					String bookData = recBooks.substring(0, recBooks.length() - 1);
					String ratingData = recRatings.substring(0, recRatings.length() - 1);
					// 针对用户对HBase表——评分推荐表进行更新
					HBaseUtils.getInstance().putTable("ratings_recTable", rowKey, "rec", "recBooks", bookData);
					HBaseUtils.getInstance().putTable("ratings_recTable", rowKey, "rec", "recRatings", ratingData);
				}
				long stop2 = System.currentTimeMillis();
				System.out.println("\n"+"本次stream更新数据："+streamUserNumber +" 条,共用时："+(stop2-start2)*1.0 / 1000 + "s");
			}
		});
		
		try {
			jssc.start();
			jssc.awaitTermination();
		}catch(InterruptedException e) {
			e.printStackTrace();
		}
	}
}
