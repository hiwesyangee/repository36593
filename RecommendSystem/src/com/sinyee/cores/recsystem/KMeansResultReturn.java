package com.sinyee.cores.recsystem;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.sinyee.cores.utils.SparkUtils;

//定义方法：输入KMeans模型参数，原始文件和新的评分，通过计算，返回新的评分相同类簇的其他随机评分的推荐结果。
public class KMeansResultReturn {

	public static String kmResult(KMeansModel kmModel, JavaRDD<String> originalLines, String newRating) {
		JavaSparkContext sc = SparkUtils.getInstance().getSparkContext();

		// 下载当天最新模型
		KMeansModel dailyKMeansModel = KMeansModel.load(sc.sc(), RecSystemProperties.KMEANS_MODEL);

		String[] array = newRating.split(",");
		double[] values = new double[array.length];
		for (int i = 0; i < array.length; i++) {
			values[i] = Double.parseDouble(array[i]);
		}
		Vector dense = Vectors.dense(values);
		// 获取这位新用户的类簇号
		int clusterNumber = dailyKMeansModel.predict(dense);

		// 根据类簇号获得同类簇其他用户的数据并返回
		JavaRDD<Vector> originalData = originalLines.map(s -> {
			String[] array2 = s.split(",");
			double[] values2 = new double[array2.length];
			for (int i = 0; i < array2.length; i++) {
				values2[i] = Double.parseDouble(array2[i]);
			}
			return Vectors.dense(values2);
		});

		JavaRDD<Vector> filter = originalData.filter(new Function<Vector, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Vector v) throws Exception {
				if (dailyKMeansModel.predict(v) == clusterNumber) {
					return true;
				}
				return false;
			}
		});
		List<Vector> take = filter.take(1);
		String emm = take.toString();
		return emm;
	}
}
