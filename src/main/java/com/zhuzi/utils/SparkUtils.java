package com.zhuzi.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.ibatis.io.Resources;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import com.google.common.reflect.Reflection;

import scala.Function1;
import scala.reflect.internal.Trees.This;

/**
 * @Title: SparkUtils.java
 * @Package com.zhuzi.booka
 * @Description: TODO(sparkUtils)
 * @author 作者 grq
 * @version 创建时间：2018年11月27日 下午2:11:02
 *
 */
public class SparkUtils {

	public static SparkSession sparkSession;
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
		SparkSession sparkSession = SparkSession.builder().appName("JavaSparkPi").master("local").config("spark.sql.shuffle.partitions", 1).getOrCreate();
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

	public static SparkContext getSparkContext() {
		return sparkSession.sparkContext();
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
				RowFactory.create();
				return RowFactory.create(record.split(fileSplit));
			}
		});

		return sparkSession.createDataFrame(rowRDD, schema);
	}

	public static <T> Dataset<T> txtfileToDateSet(SparkSession sparkSession, String filePath, String fileSplit, T t) {

		// return sparkSession.createDataFrame(rowRDD, schema);
		sparkSession.read().textFile(filePath).javaRDD().map(new Function<String, T>() {
			@Override
			public T call(String v1) throws Exception {
				String[] split = v1.split(fileSplit);

				Class<?> forName = Class.forName(t.getClass().getName());

				Object invoke = t.getClass().getMethod("build").invoke(t, split);
				T newInstance = (T) forName.newInstance();

				return null;
			}
		});

		// Encoder<LogBean> logEncoder = Encoders.bean(LogBean.class);
		return null;

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

	/**
	 * 根据文件 路径获取问价
	 * 
	 * @return
	 */
	public static String getFilePath(String filePah) {
		try {
			filePah = Resources.getResourceAsFile(filePah).getAbsolutePath();
		} catch (IOException e) {
			filePah = "data/json/person.json";
			System.out.println("获取文件失败，使用默认文件");
		}
		return filePah;

	}
}
