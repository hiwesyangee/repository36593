package com.sinyee.cores.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @ClassName: SparkUtils
 * @Description: Spark操作工具类
 * @author: hiwes
 * @date: 2018年4月13日
 */
public class SparkUtils {
	static SparkConf conf = null;
	static JavaSparkContext sc = null;
	// 私有构造方法：加载一些必要的参数
	private SparkUtils() {
		conf = new SparkConf();
//		conf.setMaster("spark://master:7077");
		conf.setMaster("local");
		conf.setAppName("RecommendSystem");
		sc =  new JavaSparkContext(conf);
	}

	private static SparkUtils instance = null;

	public static synchronized SparkUtils getInstance() {
		if (null == instance) {
			instance = new SparkUtils();
		}
		return instance;
	}
	
	/**
	 * 获取到JavaSparkContext实例
	 */
	public JavaSparkContext getSparkContext() {
		return sc;
	}
}
