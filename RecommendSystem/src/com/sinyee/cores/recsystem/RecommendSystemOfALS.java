package com.sinyee.cores.recsystem;

import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.sinyee.cores.utils.HBaseUtils;
import com.sinyee.cores.utils.SparkUtils;

/**
 * @ClassName: RecommendSystemOfALS
 * @Description: 推荐系统新架构，取缔streaming的使用，使用多线程的方式进行文件读取，测试版（第一版）
 * @author: hiwes
 * @date: 2018年4月13日
 * @version: 0.10.0
 */
public class RecommendSystemOfALS implements Serializable {
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

	// 内部类进行用户变型
	static class ParseUSer implements Function<String, String[]> {

		private static final long serialVersionUID = 1L;
		private static final Pattern COMMA = Pattern.compile(",");

		@Override
		public String[] call(String s) throws Exception {
			String[] arr = COMMA.split(s);
			String userId = arr[0]; // 用户ID
			String nickName = arr[1]; // 用户昵称
			String gender = arr[2]; // 用户性别
			String age = arr[3]; // 用户年龄
			String readStyle = arr[4]; // 用户阅读风格
			String userLabel = arr[5]; // 用户标签
			String[] array = { userId, nickName, gender, age, readStyle, userLabel };
			return array;
		}
	}

	// 内部类进行书籍变型
	static class ParseBook implements Function<String, String[]> {

		private static final long serialVersionUID = 1L;
		private static final Pattern COMMA = Pattern.compile(",");

		@Override
		public String[] call(String s) throws Exception {
			String[] arr = COMMA.split(s);
			String bookId = arr[0]; // 书籍ID
			String bookName = arr[1]; // 书籍名称
			String author = arr[2]; // 书籍作者
			String average = arr[3]; // 书籍平均分
			String bookStyle = arr[4]; // 书籍风格
			String bookLabel = arr[5]; // 书籍标签
			String[] array = { bookId, bookName, author, average, bookStyle, bookLabel };
			return array;
		}
	}

	public static void main(String[] args) {
		/**
		 * 1。 读取集群分数文件，通过划分后，进行ALS固定建模，并将ALS模型存储到HDFS， 再将推荐结果存入推荐结果表(ratings_recTable)，
		 */
		JavaSparkContext sc = SparkUtils.getInstance().getSparkContext();
		JavaRDD<String> allRatingsLines = sc.textFile(RecSystemProperties.HDFS_RATINGS_DATA); // 总体数据中的初始数据

		// 获取初始分数
		JavaRDD<Rating> originalRatings = allRatingsLines.map(new ParseRating()).cache();

		// 获取用户列表
		List<Integer> UsersList = originalRatings.map(x -> x.user()).distinct().collect();

		// 设定已经是最佳参数，并进行建模
		MatrixFactorizationModel originalALSModel = ALS.train(originalRatings.rdd(), 10, 20, 0.1);
		originalRatings.unpersist();

		// 创建评分推荐表
		String[] cfs = { "rec" };
		try {
			HBaseUtils.getInstance().createTable("ratings_recTable", cfs);
		} catch (Exception e) {
			e.printStackTrace();
		}

		for (int user : UsersList) {
			Rating[] topRatings = originalALSModel.recommendProducts(user, 10);
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
		// 保存原始ALS模型
		originalALSModel.save(sc.sc(), RecSystemProperties.ALS_MODEL);

		/**
		 * 2。 读取本地用户文件，通过划分后，将用户信息存入用户表(usersTable)， 进行KMeans建模，并将Kmeans模型存储到HDFS，
		 */
		JavaRDD<String> usersLines = sc.textFile(RecSystemProperties.HDFS_USERS_DATA).cache(); // 总体数据中的用户数据

		JavaRDD<String[]> originalUsers = usersLines.map(new ParseUSer());

		// 创建数据库用户表usersTable
		String[] cfs2 = { "info", "action", "standBy" };
		try {
			HBaseUtils.getInstance().createTable("usersTable", cfs2);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// 将用户信息保存到HBase用户表中，
		originalUsers.foreach(new VoidFunction<String[]>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String[] arr) throws Exception {
				String userId = arr[0]; // 用户ID
				String nickName = arr[1]; // 用户昵称
				String gender = arr[2]; // 用户性别
				String age = arr[3]; // 用户年龄
				String readStyle = arr[4]; // 用户阅读风格
				String userLabel = arr[5]; // 用户标签
				HBaseUtils.getInstance().putTable("usersTable", userId, "info", "nickName", nickName);
				HBaseUtils.getInstance().putTable("usersTable", userId, "info", "gender", gender);
				HBaseUtils.getInstance().putTable("usersTable", userId, "info", "age", age);
				HBaseUtils.getInstance().putTable("usersTable", userId, "info", "readStyle", readStyle);
				HBaseUtils.getInstance().putTable("usersTable", userId, "info", "userLabel", userLabel);
			}
		});

		// 分离用户id，用户性别，用户年龄
		JavaRDD<String> KMeansLines = usersLines.map(new Function<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(String user) throws Exception {
				String[] arr = user.split(",");
				String userId = arr[0]; // 用户ID
				String gender = arr[2]; // 用户性别
				String age = arr[3]; // 用户年龄
				return userId + "," + gender + "," + age;
			}
		});
		usersLines.unpersist();

		JavaRDD<Vector> kmeansVector = KMeansLines.map(s -> {
			String[] array = s.split(",");
			double[] values = new double[array.length];
			for (int i = 0; i < array.length; i++) {
				values[i] = Double.parseDouble(array[i]);
			}
			return Vectors.dense(values);
		}).cache();

		// 将用户划分为100个类
		int numClusters = 100;
		int numIterations = 20;

		// 将用户分成100个簇，并进行KMeans建模

		KMeansModel originalKMeansModel = KMeans.train(kmeansVector.rdd(), numClusters, numIterations); // 运行10次，选出最优结果
		// 保存原始KMeans模型
		originalKMeansModel.save(sc.sc(), RecSystemProperties.KMEANS_MODEL);

		/**
		 * 3。 读取本地书籍文件，通过划分后，将书籍信息存入书籍表(booksTable)，
		 */
		JavaRDD<String> booksLines = sc.textFile(RecSystemProperties.HDFS_BOOKS_DATA); // 总体数据中的书籍数据

		JavaRDD<String[]> originalBooks = booksLines.map(new ParseBook());
		// 创建数据库用户表booksTable
		String[] cfs3 = { "info", "action", "standBy" };
		try {
			HBaseUtils.getInstance().createTable("booksTable", cfs3);
		} catch (IOException e) {
			e.printStackTrace();
		}

		originalBooks.foreach(new VoidFunction<String[]>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String[] arr) throws Exception {
				String bookId = arr[0]; // 书籍ID
				String bookName = arr[1]; // 书籍名称
				String author = arr[2]; // 书籍作者
				String average = arr[3]; // 书籍平均分
				String bookStyle = arr[4]; // 书籍风格
				String bookLabel = arr[5]; // 书籍标签

				HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "bookName", bookName);
				HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "author", author);
				HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "average", average);
				HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "bookStyle", bookStyle);
				HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "bookLabel", bookLabel);
			}
		});

		// ————————————————————————初始化结束————————————————————————
		// 调用线程：
		new UserThread().start();
		new BookThread().start();
		new RatingsThread().start();
		
		//定时器任务，每天00：00进行最佳参数建模
        Calendar date = Calendar.getInstance();
        //设置时间为 xx-xx-xx 00:00:00
        date.set(date.get(Calendar.YEAR), date.get(Calendar.MONTH), date.get(Calendar.DATE), 0, 0, 0);
        //一天的毫秒数
        long daySpan = 24 * 60 * 60 * 1000;
        
		Timer timer = new Timer();
		timer.schedule(new TimerModeling(), date.getTime(), daySpan);
	}
}
