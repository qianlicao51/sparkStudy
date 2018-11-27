package com.zhuzi.booka.char03;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
	public static void main(String[] args) {
		setPart();
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
}
