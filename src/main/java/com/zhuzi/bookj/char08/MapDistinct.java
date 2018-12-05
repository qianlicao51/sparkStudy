package com.zhuzi.bookj.char08;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.zhuzi.utils.SparkUtils;

/**
 * @Title: MapDistinct.java
 * @Package com.zhuzi.bookj.char08
 * @Description: TODO(map 和falt map区别)
 * @author 作者 grq
 * @version 创建时间：2018年11月27日 下午7:31:13
 *
 */
public class MapDistinct {
	public static void main(String[] args) {

		JavaSparkContext ct = SparkUtils.getJavaSparkContext();
		JavaRDD<String> parallelize = ct.parallelize(Arrays.asList("hello", "spark", "map", "sparkMap"));

		JavaRDD<String> map = parallelize.map(t -> t + "map");

		JavaRDD<String> flatMap = parallelize.flatMap(t -> Arrays.asList(t.split("")).iterator());

		System.out.println("map:" + map.collect());

		System.out.println("flatMap:" + flatMap.collect());

	}
	// https://blog.csdn.net/icool_ali/article/details/81170526 这个在java8那本书中有
	// http://www.cnblogs.com/devin-ou/p/8028261.html

}
