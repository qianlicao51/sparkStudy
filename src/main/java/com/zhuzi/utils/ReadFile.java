package com.zhuzi.utils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @Title: ReadFile.java
 * @Package com.zhuzi.utils
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年11月29日 上午8:09:59
 *
 */
public class ReadFile {
	static JavaSparkContext sc = SparkUtils.getJavaSparkContext();
	static String fileJson = SparkUtils.getFilePath("data/json/person.json");

	public static void main(String[] args) {
		readTxt();
	}

	private static void readTxt() {
		String filePath = SparkUtils.getFilePath("data/j/char03_top.txt");
		SQLContext sqlContext = SQLContext.getOrCreate(SparkUtils.getSparkContext());
		Dataset<String> textFile = sqlContext.read().textFile(filePath);
		textFile.createOrReplaceTempView("temp_db");
		sqlContext.sql("select * from temp_db").show();
		// +-------------+
		// | value|
		// +-------------+
		// | www.a.com 90|
		// | www.b.com 1|
		// | www.c.com 39|
		// ------------------------------------------------------------------
		textFile.javaRDD().map(new Function<String, Row>() {
			@Override
			public Row call(String v1) throws Exception {
				return null;
			}
		});
	}
}
