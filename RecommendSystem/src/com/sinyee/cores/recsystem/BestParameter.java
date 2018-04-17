package com.sinyee.cores.recsystem;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

/**
 * @ClassName: BestParameter
 * @Description: 计算最佳建模参数
 * @author: hiwes
 * @date: 2018年4月9日
 */
public class BestParameter implements Serializable {
	private static final long serialVersionUID = 1L;

	private static BestParameter instance = null;

	public static synchronized BestParameter getInstance() {
		if (null == instance) {
			instance = new BestParameter();
		}
		return instance;
	}

	/**
	 * @Title: getBestParameter
	 * @Description: 计算ALS模型最佳建模参数
	 * @param trainingData
	 * @return
	 * @return 返回最佳建模参数
	 */
	public static String[] getBestParameter(JavaRDD<Rating> ratings) {

		JavaRDD<Rating>[] splits = ratings.randomSplit(new double[] { 0.7, 0.3 });
		// 训练数据
		JavaRDD<Rating> trainingData = splits[0].cache();
		// 验证数据
		JavaRDD<Rating> validationData = splits[1].cache();

		int bestRank = 0;
		int bestNumIter = 0;
		double bestLambda = 0.0;
		double bestRmse = 1.0;
		// 算法循环
		for (int numRanks = 1; numRanks <= 20; numRanks++) {
			for (int numIters = 1; numIters <= 50; numIters++) {
				for (double lambda = 0.1; lambda <= 1.0; lambda += 0.1) {
					MatrixFactorizationModel model = ALS.train(trainingData.rdd(), numRanks, numIters, lambda);
					double validationRmse = computeRmse(model, validationData);
					if (validationRmse < bestRmse) {
						bestRank = numRanks;
						bestNumIter = numIters;
						bestLambda = lambda;
						bestRmse = validationRmse;
					}
				}
			}
		}
		String[] arr = { String.valueOf(bestRank), String.valueOf(bestNumIter), String.valueOf(bestLambda) };
		return arr;
	}

	/**
	 * @Title: computeRmse
	 * @Description: 定义RMSE计算函数
	 * @param model
	 * @param data
	 * @return double
	 */
	public static double computeRmse(MatrixFactorizationModel model, JavaRDD<Rating> data) {
		JavaRDD<Rating> prediction = model.predict(data.mapToPair(new PairFunction<Rating, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Rating r) throws Exception {
				return new Tuple2<Integer, Integer>(r.user(), r.product());
			}
		}));

		JavaPairRDD<Tuple2<Object, Object>, Object> predictionRDD = prediction
				.mapToPair(new PairFunction<Rating, Tuple2<Object, Object>, Object>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Tuple2<Object, Object>, Object> call(Rating r) throws Exception {
						return new Tuple2<Tuple2<Object, Object>, Object>(
								new Tuple2<Object, Object>(r.user(), r.product()), r.rating());
					}
				});

		JavaPairRDD<Tuple2<Object, Object>, Object> dataRDD = data
				.mapToPair(new PairFunction<Rating, Tuple2<Object, Object>, Object>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Tuple2<Object, Object>, Object> call(Rating r) throws Exception {
						return new Tuple2<Tuple2<Object, Object>, Object>(
								new Tuple2<Object, Object>(r.user(), r.product()), r.rating());
					}
				});
		RDD<Tuple2<Object, Object>> predDataJoined = predictionRDD.join(dataRDD).values().rdd();
		// 返回RMSE
		return new RegressionMetrics(predDataJoined).rootMeanSquaredError();
	}
}
