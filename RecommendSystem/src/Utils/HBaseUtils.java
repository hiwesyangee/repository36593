package Utils;

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

import java.io.IOException;

/**
 * @ClassName: HBaseUtils 
 * @Description: HBase操作工具类 : 配套实时推荐系统（第八版）
 * @author: hiwes
 * @date: 2018年4月8日
 * @version： 0.8
 */
public class HBaseUtils {
	 static Configuration conf = null;
	 static  HBaseAdmin admin = null;
	 static  Connection conn = null;

	 //私有构造方法：加载一些必要的参数
	private HBaseUtils() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hy:2181");
		conf.set("hbase.rootdir", "hdfs://hy:9000/hbase");
		try {
			conn=ConnectionFactory.createConnection(conf);
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
	 * 根据表名获取到HTable实例
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
	 * @Title: creatTable
	 * @Description: 根据表名和列簇数组创建HBase表
	 * @param tableName
	 * @param familys
	 * @throws Exception
	 * @return void
	 */
	public void creatTable(String tableName, String[] familys) throws Exception {
		HBaseAdmin admin = HBaseUtils.admin;
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " finish.");
		}
	}

	/**
	 * @Title: put
	 * @Description: 根据表名、rowKey、列簇、列名和值写入HBase
	 * @param tableName
	 * @param rowKey
	 * @param cf
	 * @param column
	 * @param value
	 * @return void
	 */
	public void put(String tableName, String rowKey, String cf, String column, String value) {
		Table table = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
		try {
			table.put(put);
//			System.out.println("rowKey: " + rowKey + " is putted");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @Title: getResultTest
	 * @Description: 根据表名、rowKey、列簇和列名查找HBase数据，返回字符串
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param column
	 * @throws IOException
	 * @return String
	 */
	public String getResultTest(String tableName, String rowKey, String columnFamily, String column)throws IOException {
		Table table = getTable(tableName);
		String recResult = "";
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

			Result r = table.get(get);
			for (KeyValue kv : r.raw()) {
				recResult += new String(kv.getValue());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return recResult;
	}
	
	/**
	* @Title: get  
	* @Description: 根据表名、rowKey、列簇和列名查找HBase数据，返回字符串
	* @param tableName
	* @param rowKey
	* @param columnFamily
	* @param column
	* @throws IOException
	* @return String
	 */
	public String get(String tableName, String rowKey, String columnFamily, String column)throws IOException {
		TableName tName = TableName.valueOf(tableName);
		Table table = HBaseUtils.conn.getTable(tName);
		String recResult = "";
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
			Result r = table.get(get);
			for (KeyValue kv : r.raw()) {
				recResult += new String(kv.getValue());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return recResult;
	}
	
	public static void main(String[] args) throws Exception {
//		// 创建HBase表——评分表ratingsTable。
//		String[] cfs = { "Grade" };
//		HBaseUtils.getInstance().creatTable("ratingsTable", cfs);
//		HBaseUtils.getInstance().put("ratingsTable", "1", "Grade", "Books", "1");
//		HBaseUtils.getInstance().put("ratingsTable", "1", "Grade", "Ratings", "5.0");
//		HBaseUtils.getInstance().put("ratingsTable", "1", "Grade", "Books", "4");
//		HBaseUtils.getInstance().put("ratingsTable", "1", "Grade", "Ratings", "4.0");
//		HBaseUtils.getInstance().put("ratingsTable", "2", "Grade", "Books", "1");
//		HBaseUtils.getInstance().put("ratingsTable", "2", "Grade", "Ratings", "4.0");
//		HBaseUtils.getInstance().put("ratingsTable", "2", "Grade", "Books", "2");
//		HBaseUtils.getInstance().put("ratingsTable", "2", "Grade", "Ratings", "1.0");
//		HBaseUtils.getInstance().put("ratingsTable", "3", "Grade", "Books", "2");
//		HBaseUtils.getInstance().put("ratingsTable", "3", "Grade", "Ratings", "3.0");
//		HBaseUtils.getInstance().put("ratingsTable", "3", "Grade", "Books", "3");
//		HBaseUtils.getInstance().put("ratingsTable", "3", "Grade", "Ratings", "4.0");
//		HBaseUtils.getInstance().put("ratingsTable", "4", "Grade", "Books", "3");
//		HBaseUtils.getInstance().put("ratingsTable", "4", "Grade", "Ratings", "5.0");
//		HBaseUtils.getInstance().put("ratingsTable", "4", "Grade", "Books", "5");
//		HBaseUtils.getInstance().put("ratingsTable", "4", "Grade", "Ratings", "5.0");
//		HBaseUtils.getInstance().put("ratingsTable", "5", "Grade", "Books", "4");
//		HBaseUtils.getInstance().put("ratingsTable", "5", "Grade", "Ratings", "4.0");
//		HBaseUtils.getInstance().put("ratingsTable", "5", "Grade", "Books", "5");
//		HBaseUtils.getInstance().put("ratingsTable", "5", "Grade", "Ratings", "1.0");
//
//		// 创建HBase表——评分推荐表ratings_recTable。
//		String[] cfs2 = { "Recommend" };
//		HBaseUtils.getInstance().creatTable("ratings_recTable", cfs2);

		long start = System.currentTimeMillis();
		String result = HBaseUtils.getInstance().get("ratingsTable", "5", "Grade", "Books");
		System.out.println(result);
		long stop = System.currentTimeMillis();
		System.out.println("time=" + (stop - start) * 1.0 + "ms");
		
		long start2 = System.currentTimeMillis();
		String result2 = HBaseUtils.getInstance().get("ratingsTable", "1", "Grade", "Books");
		System.out.println(result2);
		long stop2 = System.currentTimeMillis();
		System.out.println("time=" + (stop2 - start2) * 1.0 + "ms");
	}

}