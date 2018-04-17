package com.sinyee.cores.recsystem;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import com.sinyee.cores.utils.HBaseUtils;
import com.sinyee.cores.utils.SparkUtils;

/**
 * @ClassName: BookThread
 * @Description: 新书籍信息存储线程
 * @author: hiwes
 * @date: 2018年4月15日 下午3:45:40
 */
public class BookThread extends Thread implements Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public void run() {
		/*
		 * 固定时间读取新本地文件——每隔一段时间来的用户信息（表示有新的用户加入到APP）， 通过划分后，将用户信息存储到用户表。
		 */
		JavaSparkContext sc = SparkUtils.getInstance().getSparkContext();

		while (true) {
			// 往/opt/data/transData/bookData/里传输书籍数据
			JavaRDD<String> booksLines = sc.textFile(RecSystemProperties.LOCAL_BOOKS_DATA);

			booksLines.foreach(new VoidFunction<String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void call(String line) throws Exception {
					String[] arr = line.split(",");
					String bookId = arr[0]; // 书籍ID
					String bookName = arr[1]; // 书籍名称
					String author = arr[2]; // 书籍作者
					String average = arr[3]; // 书籍平均分
					String bookStyle = arr[4]; // 书籍风格
					String bookLabel = arr[3]; // 书籍标签
					HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "bookName", bookName);
					HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "author", author);
					HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "average", average);
					HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "bookStyle", bookStyle);
					HBaseUtils.getInstance().putTable("booksTable", bookId, "info", "bookLabel", bookLabel);
				}
			});
			try {
				//每10s检测一次数据
				Thread.sleep(10000);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
}
