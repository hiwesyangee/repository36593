package com.sinyee.cores.recsystem;

import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.sinyee.cores.recsystem.RecommendSystemOfALS.ParseRating;
import com.sinyee.cores.utils.HBaseUtils;
import com.sinyee.cores.utils.SparkUtils;

import scala.Tuple2;

/**
 * @ClassName: TimerTest
 * @Description: 定时器类, 每天00：00执行模型更新任务
 * @author: hiwes
 * @date: 2018年4月17日
 */
public class TimerModeling extends TimerTask {

	@Override
	public void run() {
		// 读取本地文件进行ALS最佳参数建模
		JavaSparkContext sc = SparkUtils.getInstance().getSparkContext();

		/**
		 * 根据HDFS中收集到的评分文件进行ALS建模
		 */
		JavaRDD<String> allRatingsLines = sc.textFile(RecSystemProperties.HDFS_RATINGS_DATA); // 总体数据中的初始数据

		// 获取初始分数
		JavaRDD<Rating> originalRatings = allRatingsLines.map(new ParseRating()).cache();

		// 设定最佳参数，并进行建模
		String[] bestParameter = BestParameter.getBestParameter(originalRatings);
		int bestRank = Integer.parseInt(bestParameter[0]);
		int bestNumIter = Integer.parseInt(bestParameter[1]);
		double bestLambda = Double.parseDouble(bestParameter[2]);

		MatrixFactorizationModel originalALSModel = ALS.train(originalRatings.rdd(), bestRank, bestNumIter, bestLambda);
		originalRatings.unpersist();

		// 保存原始ALS模型
		originalALSModel.save(sc.sc(), RecSystemProperties.ALS_MODEL);

		/**
		 * 根据HBase中收集到的用户信息进行KMeans建模
		 */
		Configuration conf = HBaseUtils.getInstance().conf;
		// spark连接hbase表，并生成PairRDD
		JavaPairRDD<ImmutableBytesWritable, Result> ratingData = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
		// 获取所有的habse表ratingsTable中的评分数据
		JavaRDD<String> userInfo = ratingData.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
				String userId = Bytes.toString(t._2.getRow());
				String gender = Bytes.toString(t._2.getValue(Bytes.toBytes("userTable"), Bytes.toBytes("gender")));
				String age = Bytes.toString(t._2.getValue(Bytes.toBytes("userTable"), Bytes.toBytes("age")));
				return new String(userId + "," + gender + "," + age);
			}
		});
		JavaRDD<Vector> parsedData = userInfo.map(s -> {
			String[] array = s.split(",");
			double[] values = new double[array.length];
			for (int i = 0; i < array.length; i++) {
				values[i] = Double.parseDouble(array[i]);
			}
			return Vectors.dense(values);
		});

		// 将用户划分为100个类
		int numClusters = 100;
		int numIterations = 20;

		// 将用户分成100个簇，并进行KMeans建模

		KMeansModel originalKMeansModel = KMeans.train(parsedData.rdd(), numClusters, numIterations); // 运行10次，选出最优结果
		// 保存原始KMeans模型
		originalKMeansModel.save(sc.sc(), RecSystemProperties.KMEANS_MODEL);

	};
}
