package com.zhuzi.bookj.char01;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import com.google.common.collect.Lists;
import com.zhuzi.utils.SparkUtils;

/**
 * @Title: SortBySpark.java
 * @Package com.zhuzi.bookj.char01
 * @Description: TODO(spark排序 方案1 内存中实现二次排序)
 * @author 作者 grq
 * @version 创建时间：2018年11月28日 下午1:58:39
 *
 */
public class SortBySpark {
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		String filePath = SparkUtils.getFilePath("data/j/char01_sort.txt");

		// 步骤3 创建JavaSparkContext 对象
		JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext();
		// 步骤4 使用JavaSparkContext为上下文创建一个RDD。得到的RDD将是JavaRDD<String>
		// ,这个rdd各个元素分别是name,time,valu
		JavaRDD<String> lines = sparkContext.textFile(filePath, 1);
		for (String string : lines.collect()) {
			// System.out.println(string);

		}
		// 步骤5 从javaRDD创建键值对，键是name，值是(time,value)。得到新的RDD(为了方便我直接使用字符串)
		JavaPairRDD<String, Tuple2<String, String>> pairRDD = lines.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, Tuple2<String, String>> call(String s) throws Exception {
				String[] tokens = s.split(" ");
				Tuple2<String, String> value = new Tuple2<String, String>(tokens[1], tokens[2]);
				return new Tuple2<String, Tuple2<String, String>>(tokens[0], value);
			}
		});
		for (Tuple2<String, Tuple2<String, String>> pairTemp : pairRDD.collect()) {
			// System.out.println(pairTemp);
		}

		// 步骤 6 按照键（name）对JavaPairDD元素分组

		JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupByKey = pairRDD.groupByKey().sortByKey();
		for (JavaPairRDD<String, Iterable<Tuple2<String, String>>> temp : Lists.newArrayList(groupByKey)) {
			List<Tuple2<String, Iterable<Tuple2<String, String>>>> collect = temp.collect();
			for (Tuple2<String, Iterable<Tuple2<String, String>>> tuple2 : collect) {
				System.out.println("------step 6--->" + tuple2);
			}
		}
		// (z,[(1,4), (2,8), (3,7), (4,0)])
		// (p,[(2,6), (4,7), (1,9), (6,0), (7,3)])
		// (x,[(2,9), (1,3), (3,6)])
		// (y,[(2,5), (1,7), (3,1)])

		// 步骤7 在内存中对归约器值排序

		groupByKey.persist(StorageLevel.MEMORY_ONLY());
		JavaPairRDD<String, Iterable<Tuple2<String, String>>> soreResult = groupByKey.mapValues(new Function<Iterable<Tuple2<String, String>>, // 输入
				Iterable<Tuple2<String, String>// 输出
				>>() {
					@Override
					public Iterable<Tuple2<String, String>> call(Iterable<Tuple2<String, String>> v1) throws Exception {
						List<Tuple2<String, String>> newList = Lists.newArrayList(v1);
						Collections.sort(newList, new Tuplecompatrtor());
						return newList;
					}
				});

		List<Tuple2<String, Iterable<Tuple2<String, String>>>> collect = soreResult.collect();
		for (Tuple2<String, Iterable<Tuple2<String, String>>> temp : collect) {
			ArrayList<Tuple2<String, String>> newArrayList = Lists.newArrayList(temp._2());
			System.out.print(temp._1() + "=>[");
			for (Tuple2<String, String> temp2 : newArrayList) {
				System.out.print(temp2._2() + "  ");
			}
			System.out.println("]");
		}
		// p=>[9 6 7 0 3 ]
		// x=>[3 9 6 ]
		// y=>[7 5 1 ]
		// z=>[4 8 7 0 ]
		sparkContext.close();
		System.exit(0);
	}
}
