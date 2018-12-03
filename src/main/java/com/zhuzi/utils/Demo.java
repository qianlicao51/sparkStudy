package com.zhuzi.utils;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * @Title: Demo.java
 * @Package com.zhuzi.utils
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年12月3日 下午3:27:35
 *
 */
public class Demo {
	static JavaSparkContext sc = SparkUtils.getJavaSparkContext();

	public static void main(String[] args) {

		balnkLines();
	}

	/**
	 * 统计空白行
	 */
	private static void balnkLines() {
		final Accumulator<Integer> blandLines = sc.accumulator(0);
		JavaRDD<String> callSings = sc.textFile(SparkUtils.getFilePath("data/txt/black.TXT")).flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String t) throws Exception {
				if (t.equals("")) {
					blandLines.add(1);
				}
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		// 惰性转化操作会被 count行动操作强制触发
		callSings.count();// 没有此操作，不会真正计算
		System.out.println("blanklines>" + blandLines.value());
	}
}
