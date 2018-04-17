package com.sinyee.cores.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @function HBase工具类，使用单例模式。
 * @author hiwes
 * @data 2018年4月10日
 * @version 1.0.0
 */
public class HBaseUtils {
	public Configuration conf = null;
	public HBaseAdmin admin = null;
	public Connection conn = null;

	private HBaseUtils() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hy:2181");
		conf.set("hbase.rootdir", "hdfs://hy:8020/hbase");

		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = new HBaseAdmin(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static HBaseUtils instance = null;

	public static synchronized HBaseUtils getInstance() {
		if (null == instance) {
			instance = new HBaseUtils();
		}
		return instance;
	}

	/**
	 * 根据表名获取table实例
	 */
	public HTable getTable(String tableName) {
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return table;
	}

	/**
	 * 根据表名和列簇数组，创建HBase表
	 * 
	 * @throws IOException
	 */
	public void createTable(String tableName, String[] cfs) throws IOException {
		if (admin.tableExists(tableName)) {
			 System.out.println("table already exists!");
//			System.exit(1);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			admin.createTable(tableDesc);
		}

	}

	/**
	 * 根据表名，rowKey，列簇，列名和值，对HBase表进行写入
	 */
	public void putTable(String tableName, String rowKey, String cf, String column, String value) {
		HTable table = getTable(tableName);

		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据表名，rowKey，列簇和列名，对HBase表进行值查询
	 */
	public String getValue(String tableName, String rowKey, String cf, String column) {
		HTable table = getTable(tableName);
		String value = "";
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column));

			Result result = table.get(get);
			for (KeyValue kv : result.raw()) {
				value += new String(kv.getValue());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return value;
	}

	/**
	 * 基于评分的推荐方法，只用于——针对评分推荐表的查询 
	 * 根据用户ID，返回用户的推荐书籍结果
	 * @throws IOException
	 */
	public String getRatingsRecResult(String UId) throws IOException {
		// HTable table = getTable("ratings_recTable");
		Table table = conn.getTable(TableName.valueOf("ratings_recTable"));
		String result = "";
		Get get = new Get(Bytes.toBytes(UId));
		get.addColumn(Bytes.toBytes("rec"), Bytes.toBytes("recBooks"));
		Result r = table.get(get);
		for (KeyValue kv : r.raw()) {
			result += new String(kv.getValue());
		}

		return result;
	}

}
