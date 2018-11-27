package com.zhuzi.booka;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

/**
 * @Title: SparkUtils.java
 * @Package com.zhuzi.booka
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年11月27日 下午2:11:02
 *
 */
public class SparkUtils {

	static SparkSession sparkSession;
	static JavaSparkContext sc;
	static {
		if (sparkSession == null) {
			sparkSession = buildSparkSession();
			sc = new JavaSparkContext(sparkSession.sparkContext());
		}
	}

	/**
	 * SparkSession创建方式
	 */
	public static SparkSession buildSparkSession() {
		SparkSession sparkSession = SparkSession.builder().appName("JavaSparkPi").master("local").getOrCreate();
		return sparkSession;
	}

	/**
	 * 官方给的创建JavaSparkContext方式
	 * 
	 * @return
	 */
	public static JavaSparkContext getJavaSparkContext() {
		SparkSession sparkSession = buildSparkSession();
		SparkContext sparkContext = sparkSession.sparkContext();
		return new JavaSparkContext(sparkContext);

	}

	/**
	 * 读取文件
	 * 
	 * @param path
	 */
	public static void readFile(String path) {
		Dataset<String> textFile = sparkSession.read().textFile(path);
		JavaRDD<String> lines = textFile.toJavaRDD();
		lines.persist(StorageLevel.OFF_HEAP());
		List<String> collect = lines.collect();
		for (String strLog : collect) {
			System.out.println(strLog);
		}

	}

	/**
	 * 读取文件转为Dataset
	 * 
	 * @param sparkSession
	 * @param filePath
	 *            文件路径
	 * @param schemaString
	 *            schema 字符串(以逗号为分隔符)
	 * @param fileSplit
	 *            文件中的分隔符
	 * @return
	 */
	public static Dataset<Row> txtfileToDateSet(SparkSession sparkSession, String filePath, String schemaString, String fileSplit) {
		List<StructField> fields = new ArrayList<StructField>(16);
		for (String fieldName : schemaString.split(",")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = sparkSession.sparkContext().textFile(filePath, 1).toJavaRDD().map(new Function<String, Row>() {
			@Override
			public Row call(String record) throws Exception {
				return RowFactory.create(record.split(fileSplit));
			}
		});

		return sparkSession.createDataFrame(rowRDD, schema);
	}

	/**
	 * 读取文件转为Dataset lambda版本
	 * 
	 * @param sparkSession
	 * @param filePath
	 *            文件路径
	 * @param schemaString
	 *            schema 字符串(以逗号为分隔符)
	 * @param fileSplit
	 *            文件中的分隔符
	 * @return
	 */
	public static Dataset<Row> txtfileToDateSet2(SparkSession sparkSession, String filePath, String schemaString, String fileSplit) {
		List<StructField> fields = new ArrayList<StructField>(16);
		for (String fieldName : schemaString.split(",")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		/*
		 * <p> map(new
		 * Function<String,Row>(这个过程，不想使用java版本的方式，决定使用lambda方式，苦于无奈，
		 * 最终苦思冥想看到一个scala的demo，受到启发，使用2次map就行 <p>
		 */
		JavaRDD<Row> rowRDD = sparkSession.sparkContext().textFile(filePath, 1).toJavaRDD().map(t -> t.split("fileSplit")).map(t -> RowFactory.create(t));
		return sparkSession.createDataFrame(rowRDD, schema);
	}

}
