package com.sinyee.cores.recsystem;

import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.sinyee.cores.utils.HBaseUtils;
import com.sinyee.cores.utils.SparkUtils;

/**
 * @ClassName: RatingsThread
 * @Description: 新评分信息存储线程
 * @author: hiwes
 * @date: 2018年4月15日 下午3:46:28
 */
public class RatingsThread extends Thread implements Serializable {

	private static final long serialVersionUID = 1L;

	// 内部类进行分数变型
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

	@Override
	public void run() {
		/*
		 * 固定时间读取新本地文件——每隔一段时间来的评分信息（表示有用户产生新的评分数据），
		 * 通过划分后计算，将结果存储到推荐结果表(ratings_recTable) 并将新的用户评分数据添加入本地分数文件
		 */
		JavaSparkContext sc = SparkUtils.getInstance().getSparkContext();

		while (true) {
			// 往/opt/data/transData/ratingData/里传输书籍数据
			JavaRDD<String> ratingsLines = sc.textFile(RecSystemProperties.LOCAL_RATINGS_DATA);

			ratingsLines.foreach(new VoidFunction<String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(String str) throws Exception {
					// 将这次新的评分数据存入本地分数文件
					Scanner input = new Scanner(str);
					FileOutputStream fos = new FileOutputStream(RecSystemProperties.HDFS_RATINGS_DATA, true);
					while (input.hasNext()) {
						String a = input.next();
						fos.write((a + "\r\n").getBytes());
					}
					fos.close();
					input.close();
				}
			});

			// 数据格式转型：
			JavaRDD<Rating> newRatings = ratingsLines.map(new ParseRating()).cache();

			// 下载当天的模型
			MatrixFactorizationModel dailyALSModel = MatrixFactorizationModel.load(sc.sc(),
					RecSystemProperties.ALS_MODEL);

			JavaRDD<String> allRatingsLines = sc.textFile(RecSystemProperties.HDFS_RATINGS_DATA);
			JavaRDD<Rating> allRatings = allRatingsLines.map(new ParseRating());

			List<Integer> allUsersList = allRatings.map(x -> x.user()).distinct().collect();

			newRatings.foreach(new VoidFunction<Rating>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void call(Rating ratings) throws Exception {
					int user = ratings.user();
					int product = ratings.product();
					double rating = ratings.rating();
					// 针对分数进行一次判断，是新用户还是老用户
					if (allUsersList.contains(user)) { // 如果是老用户
						Rating[] recommendProducts = dailyALSModel.recommendProducts(user, 10);
						if (recommendProducts != null) {
							String recBooks = "";
							String recRatings = "";
							for (Rating r : recommendProducts) {
								recBooks += String.valueOf(r.product()) + ",";
								recRatings += String.valueOf(r.rating()) + ",";
							}
							String rowKey = String.valueOf(user);
							String bookData = recBooks.substring(0, recBooks.length() - 1);
							String ratingData = recRatings.substring(0, recRatings.length() - 1);
							// 针对用户对HBase表——评分推荐表进行写入
							HBaseUtils.getInstance().putTable("ratings_recTable", rowKey, "rec", "recBooks", bookData);
							HBaseUtils.getInstance().putTable("ratings_recTable", rowKey, "rec", "recRatings",
									ratingData);
						}
					} else { // 反之，是新用户
						// 下载KMeans模型，并进行比对
						String ratingss = String.valueOf(user) + "," + String.valueOf(product) + ","
								+ String.valueOf(rating);
						Scanner input = new Scanner(ratingss);
						FileOutputStream fos = new FileOutputStream(RecSystemProperties.HDFS_RATINGS_DATA, true);
						while (input.hasNext()) {
							String a = input.next();
							fos.write((a + "\r\n").getBytes());
						}
						fos.close();
						input.close();

					}
				}
			});

			try {
				//每30s检测一次数据
				Thread.sleep(30000);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
	
}
