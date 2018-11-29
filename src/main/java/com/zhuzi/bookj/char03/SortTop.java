package com.zhuzi.bookj.char03;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

import com.zhuzi.utils.SparkUtils;

/**
 * @Title: SortTop.java
 * @Package com.zhuzi.bookj
 * @Description: TODO(TOP n)
 * @author 作者 grq
 * @version 创建时间：2018年11月28日 下午4:06:13
 *
 */
public class SortTop {
	static String filePath = SparkUtils.getFilePath("data/j/char03_top.txt");

	public static void main(String[] args) {
		// sortByRDD();
		// sortBySparkSQL2();
		// sortBySparkSQL();
		sortBySparkSQL3();

	}

	private static void sortBySparkSQL3() {
		SparkSession sparkSession = SparkUtils.buildSparkSession();

		Dataset<Row> textRDD = SparkUtils.txtfileToDateSet(sparkSession, filePath, "url,lookc", " ");
		textRDD.createOrReplaceTempView("db_tmp");

		StructType schema = textRDD.schema();
		System.out.println(schema);
		StructType add = schema.add("lookc", DataTypes.IntegerType);
		
		// sparkSession.sql("select * from db_tmp order by lookc desc ").show();
		// Encoder<UrlCount> urlCountEncoder = Encoders.bean(UrlCount.class);
		// Dataset<UrlCount> as = textRDD.as(urlCountEncoder);
		// as.createOrReplaceTempView("db_tmp");
		// sparkSession.sql("select * from db_tmp order by lookc desc ").show();
	}

	private static void sortBySparkSQL2() {
		SparkSession sparkSession = SparkUtils.buildSparkSession();

		Dataset<Row> textRDD = SparkUtils.txtfileToDateSet(sparkSession, filePath, "url,lookc", " ");
		textRDD.createOrReplaceTempView("db_tmp");
		sparkSession.sql("select * from db_tmp order by lookc desc ").show();
		Encoder<UrlCount> urlCountEncoder = Encoders.bean(UrlCount.class);
		Dataset<UrlCount> as = textRDD.as(urlCountEncoder);
		as.createOrReplaceTempView("db_tmp");
		sparkSession.sql("select * from db_tmp order by lookc desc ").show();
		// 排序按照 字母顺序排序
		// +---------+-----+
		// | url|lookc|
		// +---------+-----+
		// |www.b.com| 1|
		// |www.j.com| 100|
		// |www.k.com| 101|
		// |www.l.com| 103|
		// |www.m.com| 105|
		// |www.n.com| 109|
		// |www.d.com| 15|
		// |www.g.com| 23|
		// |www.i.com| 37|
		// |www.c.com| 39|
		// |www.h.com| 65|
		// |www.e.com| 72|
		// |www.p.com| 812|
		// |www.f.com| 83|
		// |www.a.com| 90|
		// |www.o.com| 97|
		// +---------+-----+

	}

	/**
	 * 为何不使用SparkSQL
	 */
	@SuppressWarnings("unchecked")
	private static void sortBySparkSQL() {

		SparkSession session = SparkUtils.buildSparkSession();
		JavaRDD<UrlCount> map = session.read().textFile(filePath).javaRDD().map(new Function<String, UrlCount>() {
			@Override
			public UrlCount call(String v1) throws Exception {
				UrlCount urlCount = new UrlCount();
				urlCount.setUrl(v1.split(" ")[0]);
				urlCount.setLookc(Integer.valueOf(v1.split(" ")[1]));
				return urlCount;
			}
		});
		Dataset<Row> createDataFrame = session.createDataFrame(map, UrlCount.class);
		createDataFrame.createOrReplaceTempView("temp_db");
		session.sql("select * from temp_db t order by t.lookc desc").show();

		session.close();
	}

	private static void sortByRDD() {
		// step1 创建sparkContext 读取文件作为RDD
		String filePath = SparkUtils.getFilePath("data/j/char03_top.txt");

		// 步骤3 创建JavaSparkContext 对象
		JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext();
		JavaRDD<String> lines = sparkContext.textFile(filePath, 3);
		// step2 创建tuple<Integer ,String>
		JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(t -> (new Tuple2<String, Integer>(t.split(" ")[0], Integer.valueOf(t.split(" ")[1]))));
		// step3 为各个输入分区创建 一个本地top 10
		// pairRDD.mapPartitions(new
		// FlatMapFunction<Iterator<Tuple2<String,Integer>>,sortmap >() {

		int size = pairRDD.partitions().size();
		// step4 收集本地top 10创建最终的top 10
		System.out.println(size);
	}

	static public class UrlCount {
		private String url;
		private Integer lookc;

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public Integer getLookc() {
			return lookc;
		}

		public void setLookc(Integer lookc) {
			this.lookc = lookc;
		}
	}
}
