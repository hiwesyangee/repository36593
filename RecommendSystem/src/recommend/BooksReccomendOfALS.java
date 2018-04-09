package recommend;

import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

import Utils.HBaseUtils;
import Utils.SparkUtils;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

import scala.Tuple2;

public class BooksReccomendOfALS {
	// 1. 定义一个评级启发函数。

	// 2. 定义一个RMSE计算函数。
	public static double computeRmse(MatrixFactorizationModel model, RDD<Rating> data) {
		JavaRDD<Rating> prediction = model
				.predict(data.toJavaRDD().mapToPair(new PairFunction<Rating, Integer, Integer>() {
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

		JavaPairRDD<Tuple2<Object, Object>, Object> dataRDD = data.toJavaRDD()
				.mapToPair(new PairFunction<Rating, Tuple2<Object, Object>, Object>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Tuple2<Object, Object>, Object> call(Rating r) throws Exception {
						return new Tuple2<Tuple2<Object, Object>, Object>(
								new Tuple2<Object, Object>(r.user(), r.product()), r.rating());
					}
				});
		RDD<Tuple2<Object, Object>> predDataJoined = predictionRDD.join(dataRDD).values().rdd();
		// 定义一个最佳RMSE
		return new RegressionMetrics(predDataJoined).rootMeanSquaredError();
	}

	// 3. 运行函数主体
	public static void run(String[] args) {
		// 3.1 设置连接
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		if (args.length != 1) {
			System.out.print("Usage: BooksLensHomeDir");
			System.exit(1);
		}

		SparkConf conf = SparkUtils.getInstance().conf.set("spark.executor.memory", "500m");
		JavaSparkContext sc = SparkUtils.getInstance().sc;

		// 3.2 加载评级数据并了解数据。
		String booksLensHomeDir = args[0];
		// 根据已有的评分数据文件，得到JavaRDD<Rating>：评分RDD。（以逗号进行划分）
		JavaRDD<Rating> ratings = sc.textFile(new File(booksLensHomeDir, "ratings.dat").toString())
				.map(new Function<String, Rating>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Rating call(String s) throws Exception {
						String[] arr = s.split(",");
						int user = Integer.parseInt(arr[0]);
						int product = Integer.parseInt(arr[1]);
						double rating = Double.parseDouble(arr[2]);
						return new Rating(user, product, rating);
					}
				});

		// 根据已有的书籍文件，得到Map<Integer,String>：书籍信息Map。（以逗号进行划分）
		Map<Integer, String> books = sc.textFile(new File(booksLensHomeDir, "books.dat").toString())
				.mapToPair(new PairFunction<String, Integer, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(String s) throws Exception {
						String[] arr = s.split(",");
						int movieId = Integer.parseInt(arr[0]);
						String movieName = arr[1];
						return new Tuple2<Integer, String>(movieId, movieName);
					}
				}).collectAsMap();

		// 获得评分个数（即所有用户的多有评分条数）。
		long numRatings = ratings.count();
		// 得到用户的数量
		long numUser = ratings.map(new Function<Rating, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Rating r) throws Exception {
				return r.user();
			}
		}).distinct().count();

		// 得到书籍的数量
		long numBooks = ratings.map(new Function<Rating, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Rating r) throws Exception {
				return r.product();
			}
		}).distinct().count();

		// 显示评分条数，用户数目，书籍数目。
		System.out.println("Got " + numRatings + " ratings from " + numUser + " users on " + numBooks + " books.");

		// 3.3 将数据分成训练(60%)、验证(20%)和测试(20%)
		JavaRDD<Rating>[] splits = ratings.randomSplit(new double[] { 0.6, 0.2, 0.2 });
		// 训练数据
		JavaRDD<Rating> trainData = splits[0].cache();
		// 验证数据
		JavaRDD<Rating> validationData = splits[1].cache();
		// 测试数据
		JavaRDD<Rating> testData = splits[2].cache();

		long numTrain = trainData.count();
		long numValidation = validationData.count();
		long numTest = testData.count();
		System.out.println(
				"Training data: " + numTrain + ", Validation data: " + numValidation + ", Test data: " + numTest);

		// 3.4 训练模型，并对模型进行验证。
		int bestRank = 0;
		int bestIter = 0;
		double bestLambda = 0.0;
		double bestRmse = 1.0;
		MatrixFactorizationModel bestModel = null;
		for (int numRanks = 9; numRanks <= 12; numRanks++) {
			for (int numIters = 10; numIters <= 20; numIters++) {
				for (double numLambdas = 0.1; numLambdas <= 10.0; numIters += 0.1) {
					MatrixFactorizationModel model = ALS.train(trainData.rdd(), numRanks, numIters, numLambdas);
					double validationRmse = computeRmse(model, validationData.rdd());
					if (validationRmse < bestRmse) {
						bestRank = numRanks;
						bestIter = numIters;
						bestLambda = numIters;
						bestRmse = validationRmse;
						bestModel = model;
					}
					System.out.println();
				}
				System.out.println("the bestRank = " + bestRank + ",  the  bestLambda = " + bestLambda
						+ ",  on model,RMSE is " + bestRmse);
			}
		}

		// 3.5 在测试集上评估模型。
		double testRmse = computeRmse(bestModel, testData.rdd());
		System.out.println("The best model was trained with rank=" + bestRank + ",Iter=" + bestIter + ", Lambda="
				+ bestLambda + " and compute RMSE on test is " + testRmse);

		// 3.6 创建基线，并将其与最佳模型进行比较。
		MatrixFactorizationModel model = ALS.train(ratings.rdd(), bestRank, bestIter, bestLambda);

		JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(r -> new Tuple2<>(r.user(), r.product()));
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD
				.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
						.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())));
		JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD
				.fromJavaRDD(ratings.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
				.join(predictions).values();
		double MSE = ratesAndPreds.mapToDouble(pair -> {
			double err = pair._1() - pair._2();
			return err * err;
		}).mean();

		RDD<Tuple2<Object, Object>> map = testData.map(new Function<Rating, Tuple2<Object, Object>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Object, Object> call(Rating r) throws Exception {
				return new Tuple2<Object, Object>(r.rating(), MSE);
			}
		}).rdd();

		double improvement = (new RegressionMetrics(map).rootMeanSquaredError() - testRmse)
				/ new RegressionMetrics(map).rootMeanSquaredError() * 100;
		System.out.println("The best model improves the baseline by " + "%1.2f" + improvement + "%.");

		// 3.7 得到个性化推荐结果
		for (int user = 1; user <= (int) numUser; user++) {
			Rating[] topRatings = bestModel.recommendProducts(user, 10);
			String books1 = "";
			String ratings1 = "";
			for (Rating r : topRatings) {
				books1 += String.valueOf(r.product()) + ",";
				ratings1 += String.valueOf(r.rating()) + ",";
			}
			String data1 = books1.substring(0, books1.length() - 1);
			String data2 = ratings1.substring(0, ratings1.length() - 1);
			HBaseUtils.getInstance().put("ratings_recTable", String.valueOf(user), "r", "recBooks", data1);
			HBaseUtils.getInstance().put("ratings_recTable", String.valueOf(user), "r", "recRatings", data2);
		}
	}
}
