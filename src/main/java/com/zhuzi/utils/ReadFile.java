package com.zhuzi.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
	static String filePath = SparkUtils.getFilePath("data/j/char03_top.txt");
	static SparkSession sparkSession = SparkUtils.buildSparkSession();

	public static void main(String[] args) {
		// readJson();
		System.out.println(sparkSession.conf().getAll());
	}

	/**
	 * 读取json SparkSession 读取json文件还排序还是可以按 数字排序的
	 */
	private static void readJson() {
		SparkSession sparkSession = SparkUtils.buildSparkSession();
		Dataset<Row> json = sparkSession.read().json(fileJson);
		json.createOrReplaceTempView("temp_db");
		sparkSession.sql("select * from temp_db order by  sal desc limit 10").show();
		// +---+------+------+----------+--------+-------+
		// |age|deptno|gender|job number| name| sal|
		// +---+------+------+----------+--------+-------+
		// | 5| 1| male| 104| zhaoliu|2800000|
		// | 36| 3|female| 105| tianqi| 90000|
		// | 35| 3|female| 103| wangwu| 50000|
		// | 30| 2| male| 102| lisi| 20000|
		// | 33| 1| male| 101|zhangsan| 18000|
		// +---+------+------+----------+--------+-------+
		Dataset<String> textFile = sparkSession.read().textFile(filePath);
		textFile.createOrReplaceTempView("temp");
		sparkSession.sql("select * from temp").show();

	}

	private static void readTextFile() {
		List<StructField> fields = new ArrayList<StructField>(16);
		for (String fieldName : "url,count".split(",")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		/*
		 * <p> map(new
		 * Function<String,Row>(这个过程，不想使用java版本的方式，决定使用lambda方式，苦于无奈，
		 * 最终苦思冥想看到一个scala的demo，受到启发，使用2次map就行 <p>
		 */
		JavaRDD<Row> rowRDD = sparkSession.sparkContext().textFile(filePath, 1).toJavaRDD().map(t -> t.split(" ")).map(t -> RowFactory.create(t));
		Dataset<Row> dataFrame = sparkSession.createDataFrame(rowRDD, schema);
		dataFrame.createOrReplaceTempView("temp_db");
		sparkSession.sql("select * from temp_db order by count desc limit 5").show();
		// +---------+-----+
		// | url|count|
		// +---------+-----+
		// |www.o.com| 97|
		// |www.a.com| 90|
		// |www.f.com| 83|
		// |www.p.com| 812|
		// |www.e.com| 72|
		// +---------+-----+
	}

	/**
	 * 以文本文件的格式读取文本文件
	 */
	private static void readTxtByStr() {

		Dataset<Row> textFile = sparkSession.read().text(filePath).toDF();
		textFile.createOrReplaceTempView("temp_db");

		sparkSession.sql("select * from temp_db").show();
		// +-------------+
		// | value|
		// +-------------+
		// | www.a.com 90|
		// | www.b.com 1|
		// | www.c.com 39|
		// | www.d.com 15|
		// | www.e.com 72|
		RDD<String> textFile2 = sparkSession.sparkContext().textFile(fileJson, 1);

	}

	/**
	 * 以java类型的格式读取文本文件
	 */
	private static void readTxtByBean() {
		SQLContext sqlContext = SQLContext.getOrCreate(SparkUtils.getSparkContext());
		Dataset<String> textFile = sqlContext.read().textFile(filePath);
		// ------------------------------------------------------------------
		JavaRDD<Row> map = textFile.javaRDD().map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String v1) throws Exception {
				String[] tokens = v1.split(" ");
				return RowFactory.create(tokens[0], Integer.parseInt(tokens[1]));
			}
		});

		List<StructField> fields = new ArrayList<StructField>(16);
		fields.add(DataTypes.createStructField("url", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("count", DataTypes.IntegerType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataFrame = sqlContext.createDataFrame(map, schema);
		dataFrame.createOrReplaceTempView("temp_db");

		sqlContext.sql("select * from temp_db order by count desc limit 10").show();
		// +---------+-----+
		// | url|count|
		// +---------+-----+
		// |www.p.com| 812|
		// |www.n.com| 109|
		// |www.m.com| 105|
		// |www.l.com| 103|
		// |www.k.com| 101|
		// |www.j.com| 100|
		// |www.o.com| 97|
		// |www.a.com| 90|
		// |www.f.com| 83|
		// |www.e.com| 72|
		// +---------+-----+
	}
}
