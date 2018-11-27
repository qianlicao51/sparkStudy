package com.zhuzi.booka.char03;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

import com.zhuzi.utils.SparkUtils;

/**
 * @Title: Demo1.java
 * @Package com.zhuzi.booka.char03
 * @Description: TODO(第三章例子)
 * @author 作者 grq
 * @version 创建时间：2018年11月27日 下午2:58:43
 *
 */
public class Demo01 {
	static JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext();

	public static void main(String[] args) {
		// parBy(sparkContext);
		transFlatMap();
	}

	/**
	 * 想模拟宽窄依赖，但是不知道java版本对于的API放弃
	 * 
	 * @param sparkContext
	 */
	private static void parBy(JavaSparkContext sparkContext) {
		JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
		JavaRDD<Tuple2<Integer, Integer>> mapRDD = rdd.map(t -> new Tuple2<Integer, Integer>(t, t));

	}

	/**
	 * 设置分区数量
	 */
	private static void setPart() {
		JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext();
		// // 指定RDD分区数量 4个分区
		JavaRDD<String> rdd = sparkContext.parallelize(Arrays.asList("hello", "spark"), 4);
		int size = rdd.partitions().size();
		System.out.println("分区数量是：" + size);
		// 分区数量是：4
	}

	static void buildRDD() {
		JavaRDD<Integer> parallelize = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		System.out.println(parallelize.collect());
	}

	/**
	 * 示例 3-7 创建分区数为4的RDD
	 */
	static void readFileToRDD() {
		String filePath = "data/txt/20.TXT";
		JavaRDD<String> textFile = sparkContext.textFile(SparkUtils.getFilePath(filePath), 4);
		System.out.println(textFile.partitions().size());
	}

	/**
	 * 例3-9
	 */
	static void transMapValue() {
		JavaRDD<String> rdd = sparkContext.parallelize(Arrays.asList("dog", "tiger", "loin", "cat", "panther", "eagle"));
		JavaRDD<Tuple2<Integer, String>> map = rdd.map(t -> new Tuple2<Integer, String>(t.length(), t));
		System.out.println(map.collect());
	}

	/**
	 * 例 3-11.flatMap
	 */
	static void transFlatMap() {
		JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 3));

		JavaRDD<String> flatMap = rdd.flatMap(new FlatMapFunction<Integer, String>() {
			@Override
			public Iterator<String> call(Integer t) throws Exception {
				return Arrays.asList(t + "<>" + t).iterator();
			}
		});
		System.out.println(flatMap.collect());
		JavaRDD<String> map = rdd.map(t -> t + "<>" + t);
		System.out.println(map.collect());
	}
}
