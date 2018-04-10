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
 * @Description: 初版推荐系统(正式版)最佳参数计算
 * @function: 计算最佳参数，通过对RDD进行循环建模，得到最佳建模参数
 * @author: hiwes
 * @data: 2018年4月10日
 * @version: 1.0.0
 */
public class BestParameter implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * @Title: getBestParameter
	 * @Description: 计算ALS建模最佳参数
	 * @return : String[] 返回最佳参数数组，{rank, iteration, lambda}
	 */
	public String[] getBestParameter(JavaRDD<Rating> ratings) {
		// 对数据集进行切分，训练数据70%，测试数据30%
		JavaRDD<Rating>[] splits = ratings.randomSplit(new double[] { 0.7, 0.3 });
		// 训练数据
		JavaRDD<Rating> trainData = splits[0].cache();
		// 测试数据
		JavaRDD<Rating> validationData = splits[1].cache();

		int bestNumRank = 0;
		int bestNumIter = 0;
		double bestLambda = 0.0;
		double bestRmse = 1.0;

		// 通过循环，计算最佳参数
		for (int numRank = 1; numRank <= 20; numRank++) {
			for (int numIter = 5; numIter <= 20; numIter++) {
				for (double lambda = 0.1; lambda <= 1.0; lambda += 0.1) {
					// 针对当前参数建模
					MatrixFactorizationModel model = ALS.train(trainData.rdd(), numRank, numIter, lambda);
					// 调用方法，计算RMSE值
					double validationRmse = computeRmse(model, validationData);
					if (validationRmse < bestRmse) {
						bestNumRank = numRank;
						bestNumIter = numIter;
						bestLambda = lambda;
						bestRmse = validationRmse;
					}
				}
			}
		}

		// 将最佳参数存入数组
		String[] bestParameter = { String.valueOf(bestNumRank), String.valueOf(bestNumIter), String.valueOf(bestLambda),
				String.valueOf(bestRmse) };
		
		
		return bestParameter;
	}

	/**
	 * @Title: computeRmse
	 * @Description: 计算RMSE均方根误差
	 * @return : double 返回RMSE均方根误差值
	 */
	public double computeRmse(MatrixFactorizationModel model, JavaRDD<Rating> rdd) {
		// 针对模型和RDD，进行分数预测
		JavaRDD<Rating> prediction = model.predict(rdd.mapToPair(new PairFunction<Rating, Integer, Integer>() {

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

		JavaPairRDD<Tuple2<Object, Object>, Object> dataRDD = rdd
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
