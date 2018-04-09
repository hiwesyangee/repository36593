package Utils;

import java.io.IOException;

public class test {
	public static void main(String[] args) throws IOException {
		long start1 = System.currentTimeMillis();
		HBaseUtils.getInstance().get("ratings_recTable", "1", "r", "recBooks");
		long stop1 = System.currentTimeMillis();
		System.out.println("time=" + (stop1 - start1) * 1.0 + "ms");

		long start2 = System.currentTimeMillis();
		HBaseUtils.getInstance().get("ratings_recTable", "2", "r", "recBooks");
		long stop2 = System.currentTimeMillis();
		System.out.println("time=" + (stop2 - start2) * 1.0 + "ms");

		long start3 = System.currentTimeMillis();
		HBaseUtils.getInstance().get("ratings_recTable", "3", "r", "recBooks");
		long stop3 = System.currentTimeMillis();
		System.out.println("time=" + (stop3 - start3) * 1.0 + "ms");

		long start4 = System.currentTimeMillis();
		HBaseUtils.getInstance().get("ratings_recTable", "4", "r", "recBooks");
		long stop4 = System.currentTimeMillis();
		System.out.println("time=" + (stop4 - start4) * 1.0 + "ms");

		long start5 = System.currentTimeMillis();
		HBaseUtils.getInstance().get("ratings_recTable", "5", "r", "recBooks");
		long stop5 = System.currentTimeMillis();
		System.out.println("time=" + (stop5 - start5) * 1.0 + "ms");
		
	}
}
