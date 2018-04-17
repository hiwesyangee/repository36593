package com.sinyee.cores.recsystem;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.sinyee.cores.utils.HBaseUtils;
import com.sinyee.cores.utils.SparkUtils;

/**
 * @ClassName: UserThread
 * @Description: 新用户信息存储线程
 * @author: hiwes
 * @date: 2018年4月15日 下午3:45:13
 */
public class UserThread extends Thread implements Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public void run() {
		/*
		 * 固定时间读取新本地文件——每隔一段时间来的用户信息（表示有新的用户加入到APP）， 通过划分后，将用户信息存储到用户表。
		 */
		JavaSparkContext sc = SparkUtils.getInstance().getSparkContext();

		while (true) {
			// 往/opt/data/transData/userData/里传输用户数据
			JavaRDD<String> usersLines = sc.textFile(RecSystemProperties.LOCAL_USERS_DATA);

			// 实现用户信息转化：获得uId，gender，age
			JavaRDD<String> data = usersLines.map(new Function<String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public String call(String line) throws Exception {
					String[] arr = line.split(",");
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
					String userInfo = userId + "," + gender + "," + age;
					return userInfo;
				}
			});

			// 实现新用户注册后的初始化
			JavaRDD<Vector> parsedData = data.map(s -> {
				String[] array = s.split(",");
				double[] values = new double[array.length];
				for (int i = 0; i < array.length; i++) {
					values[i] = Double.parseDouble(array[i]);
				}
				return Vectors.dense(values);
			}).cache();

			// 下载HDFS上的KMeans模型
			KMeansModel dailyKMeansModel = KMeansModel.load(sc.sc(), RecSystemProperties.KMEANS_MODEL);

			JavaRDD<String> ratingsLines = sc.textFile(RecSystemProperties.HDFS_RATINGS_DATA).cache();

			// 将每个用户进行KMeans模型判断，获得
			parsedData.foreach(new VoidFunction<Vector>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void call(Vector v) throws Exception {
					String userInfo = v.toString();
					String user = userInfo.split(",")[0];
					// 利用KMeans模型，整体ratings文件，用户信息
					String similarUser = KMeansResultReturn.kmResult(dailyKMeansModel, ratingsLines, userInfo);

					String[] split = similarUser.split(",");
					String usersss = split[0];
					String newResult = HBaseUtils.getInstance().getRatingsRecResult(usersss);

					String rowKey = String.valueOf(user);
					HBaseUtils.getInstance().putTable("ratings_recTable", rowKey, "rec", "recBooks", newResult);
				}
			});

			ratingsLines.unpersist();

			try {
				//每10s检测一次数据
				Thread.sleep(10000);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

}
