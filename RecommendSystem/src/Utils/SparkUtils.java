package Utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * @ClassName: SparkUtils 
 * @Description: Spark操作工具类
 * @author: hiwes
 * @date: 2018年4月4日
 */
public class SparkUtils {

	public SparkConf conf = null;
	public JavaSparkContext sc = null;
	public JavaStreamingContext jssc = null;
	
	/**
	 * 私有构造方法：加载一些必要的参数
	 */
	private SparkUtils() {
		conf = new SparkConf();
		conf.setMaster("local[2]");
		conf.setAppName("RecommendSystem");

		sc = new JavaSparkContext(conf);
		jssc = new JavaStreamingContext(sc, Durations.seconds(30));
		jssc.checkpoint("/opt/tmp/checkpoint");
	}

	private static SparkUtils instance = null;

	public static synchronized SparkUtils getInstance() {
		if (null == instance) {
			instance = new SparkUtils();
		}
		return instance;
	}

	/**
	 * @Title: getDataFromHBase
	 * @Description: 将一个RDD进行HBase交互操作。
	 * @param rdd
	 * @throws IOException
	 * @return void
	 */
	public static JavaRDD<Rating> loadDataFromHBase() throws IOException {
		// 连接hbase并下载数据
		Configuration hbaseConf = HBaseUtils.conf;
		// 连接hbase表：usertableTest
		hbaseConf.set(TableInputFormat.INPUT_TABLE, "ratingsTable");
		Scan scan = new Scan();
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		String ScanToString = Base64.encodeBytes(proto.toByteArray());
		hbaseConf.set(TableInputFormat.SCAN, ScanToString);
		// spark连接hbase表，并生成PairRDD
		JavaPairRDD<ImmutableBytesWritable, Result> ratingData = SparkUtils.getInstance().sc.newAPIHadoopRDD(hbaseConf,
				TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		// 获取所有的habse表ratingsTable中的评分数据
		JavaRDD<Rating> hbaseRatings = ratingData.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Rating>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Rating call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
				Result result = t._2();
				String userId = Bytes.toString(result.getRow());

				String recResult = Bytes
						.toString(result.getValue(Bytes.toBytes("ratingsTable"), Bytes.toBytes("ratings")));
				String[] ratingsSplit = recResult.split(",");
				String product = "";
				String ratings = "";
				for (String s : ratingsSplit) {
					product = s.split(":")[0];
					ratings = s.split(":")[1];
					return new Rating(Integer.parseInt(userId), Integer.parseInt(product), Double.parseDouble(ratings));
				}
				return null;
			}
		});
		return hbaseRatings;
	}

	/**
	 * @Title: loadValueFromHBase
	 * @Description: 下载hbase中评分表的值（就是对哪些书进行过评分的字符串）
	 * @return
	 * @throws IOException
	 * @return JavaRDD<Rating>
	 */
	public static String loadValueFromHBase() throws IOException {
		// 连接hbase并下载数据
		Configuration hbaseConf = HBaseUtils.getInstance().conf;
		// 连接hbase表：usertableTest
		hbaseConf.set(TableInputFormat.INPUT_TABLE, "ratingsTable");
		Scan scan = new Scan();
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		String ScanToString = Base64.encodeBytes(proto.toByteArray());
		hbaseConf.set(TableInputFormat.SCAN, ScanToString);
		// spark连接hbase表，并生成PairRDD
		JavaPairRDD<ImmutableBytesWritable, Result> ratingData = SparkUtils.getInstance().sc.newAPIHadoopRDD(hbaseConf,
				TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		// 获取所有的habse表ratingsTable中的评分数据
		JavaRDD<String> hbaseValues = ratingData.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
				Result result = t._2();
				String recResult = Bytes
						.toString(result.getValue(Bytes.toBytes("ratingsTable"), Bytes.toBytes("ratings")));
				return recResult;
			}
		});
		List<String> collect = hbaseValues.collect();
		for (String s : collect) {
			return s;
		}
		return null;
	}
}
