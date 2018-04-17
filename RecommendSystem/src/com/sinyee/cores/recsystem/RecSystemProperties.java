package com.sinyee.cores.recsystem;

public class RecSystemProperties {

	// 评分数据集群目录
	public static final String HDFS_RATINGS_DIRECTORY = "hdfs://hy:8020/data/overallData/originalRatings.dat";
	// 评分数据集群文件
	public static final String HDFS_RATINGS_DATA = "hdfs://hy:8020/data/overallData/originalRatings.dat";
	// 用户数据集群文件
	public static final String HDFS_USERS_DATA = "hdfs://hy:8020/data/overallData/usersInfo.dat";
	// 书籍数据集群文件
	public static final String HDFS_BOOKS_DATA = "hdfs://hy:8020/data/overallData/booksInfo.dat";

	// ALS模型保存位置
	public static final String ALS_MODEL = "hdfs://hy:8020/model/allModel/ALSModel";
	// KMeans模型保存位置
	public static final String KMEANS_MODEL = "hdfs://hy:8020/model/allModel/KMeansModel";

	// 评分数据监控文件/目录
	public static final String LOCAL_RATINGS_DATA = "/opt/data/transData/ratingData/users.dat";
	// 用户数据监控文件/目录
	public static final String LOCAL_USERS_DATA = "/opt/data/transData/userData/usersInfo.dat";
	// 用户数据监控文件/目录
	public static final String LOCAL_BOOKS_DATA = "/opt/data/transData/userData/booksInfo.dat";

}
