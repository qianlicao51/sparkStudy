package com.zhuzi.bookj.char04;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.zhuzi.utils.SparkUtils;

/**
 * @Title: LeftSpark.java
 * @Package com.zhuzi.bookj.char04
 * @Description: TODO(左外连接)
 * @author 作者 grq
 * @version 创建时间：2018年12月3日 下午5:08:24
 *
 */
public class LeftSpark {
	public static void main(String[] args) {
		String userText = SparkUtils.getFilePath("data/j/char04_user.txt");
		String tranText = SparkUtils.getFilePath("data/j/char04_trans.txt");
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();

		SparkSession sparkSession = SparkUtils.buildSparkSession();
		JavaRDD<String> userRDD = sc.textFile(userText, 1);

		Dataset<Row> userDF = SparkUtils.txtfileToDateSet(sparkSession, userText, "user_id,location_id", "\t");

		Dataset<Row> transDF = SparkUtils.txtfileToDateSet(sparkSession, tranText, "tran_id,prod_id,user_id,quantity,amount", "\t");

		userDF.createOrReplaceTempView("user");

		transDF.createOrReplaceTempView("trans");

		Dataset<Row> sql = sparkSession.sql("select t.tran_id,t.prod_id,u.location_id from trans t left join user u on t.user_id=u.user_id");
		sql.show();
		// +-------+-------+-----------+
		// |tran_id|prod_id|location_id|
		// +-------+-------+-----------+
		// | t1| p3| UT|
		// | t3| p1| UT|
		// | t6| p1| UT|
		// | t7| p4| UT|
		// | t2| p1| GA|
		// | t4| p2| GA|
		// | t5| p4| CA|
		// | t8| p4| GA|
		// +-------+-------+-----------+

	}
}
