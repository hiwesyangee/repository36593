package Utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URI;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * @ClassName: HDFSUtils 
 * @Description: HDFS操作工具类
 * @author: hiwes
 * @date: 2018年4月4日
 * @version： 0.1
 */
public class HdfsUtils {
		public Configuration conf = null;
		public FileSystem fs = null;
		
		/**
		 * 私有构造方法：加载一些必要的参数
		 */
		private HdfsUtils() {
			conf = new Configuration();
			try {
				fs = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private static HdfsUtils instance = null;

		public static synchronized HdfsUtils getInstance() {
			if (null == instance) {
				instance = new HdfsUtils();
			}
			return instance;
		}
		
		/**
		* 通过路径读取hdfs文件
		 */
		public static void readHdfsURL(String url) throws IOException{
			InputStream in = null;
	        try {
	            in = new URL(url).openStream();
	            IOUtils.copyBytes(in, System.out, 4096, false);
	        } finally {
	            IOUtils.closeStream(in);
	        }
		}
		
		/**
		* 通过API读取hdfs文件
		 */
		public static void readHdfsAPI(String url) throws IOException{
			Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(URI.create(url), conf);
	        InputStream in = null;
	        try {
	            in = fs.open(new Path(url));
	            IOUtils.copyBytes(in, System.out, 4096, false);
	        } finally {
	            IOUtils.closeStream(in);
	        }
		}
		
		/**
		* 写数据
		 */
		public static void  fileCopy(String localFile, String hdfsFile) throws IOException{
			InputStream in = new BufferedInputStream(new FileInputStream(localFile));
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(URI.create(hdfsFile),conf);
	        OutputStream out  = fs.create(new Path(hdfsFile),new Progressable(){
	            public void progress(){
	                System.out.println("*");
	            }
	        });
	        IOUtils.copyBytes(in, out, 4096,true);
		}
		
		/**
		* 文件系统查询 
		*/
		public static void readStatus(String url) throws IOException {
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(URI.create(url), conf);
	        Path[] paths = new Path[1];
	        paths[0] = new Path(url);
	        FileStatus[] status = fs.listStatus(paths);
	        Path[] listedPaths = FileUtil.stat2Paths(status);
	        for (Path p : listedPaths) {
	            System.out.println(p);
	        }
	    }
		
		
}
