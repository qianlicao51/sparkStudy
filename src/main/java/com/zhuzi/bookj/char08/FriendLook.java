package com.zhuzi.bookj.char08;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.google.common.collect.Lists;
import com.zhuzi.utils.SparkUtils;

/**
 * @Title: FriendLook.java
 * @Package com.zhuzi.bookj.char08
 * @Description: TODO(查询共同好友)
 * @author 作者 grq
 * @version 创建时间：2018年11月27日 下午6:48:55
 *
 */
public class FriendLook {
	public static void main(String[] args) {

		friends();
	}

	static void friends() {
		// 数据文件路径
		String filePath = SparkUtils.getFilePath("data/j/char08_friend.txt");

		JavaSparkContext ctx = SparkUtils.getJavaSparkContext();

		// 步骤4 创建第一个javaRDD表示输入文件
		JavaRDD<String> records = ctx.textFile(filePath, 1);

		// 步骤5 应用映射器 此处是 faltMap而不是map
		JavaPairRDD<Tuple2<String, String>, Iterable<String>> pairs = records.flatMapToPair(new PairFlatMapFunction<String, Tuple2<String, String>, // KEY
				Iterable<String>// VALUES
				>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<Tuple2<String, String>, Iterable<String>>> call(String t) throws Exception {
						String[] tokens = t.split(",");
						String personID = tokens[0];
						String[] friendsTokens = tokens[1].split(" ");

						if (friendsTokens.length == 1) {
							Tuple2<String, String> key = buildSortPair(personID, friendsTokens[0]);
							Tuple2<Tuple2<String, String>, Iterable<String>> tempResult = new Tuple2<Tuple2<String, String>, Iterable<String>>(key, new ArrayList<String>());
							return Arrays.asList(tempResult).iterator();
						}
						List<String> friends = Arrays.asList(friendsTokens);

						List<Tuple2<Tuple2<String, String>, Iterable<String>>> result = new ArrayList<Tuple2<Tuple2<String, String>, Iterable<String>>>();
						for (String f : friends) {
							Tuple2<String, String> key = buildSortPair(personID, f);
							Tuple2<Tuple2<String, String>, Iterable<String>> tuple2 = new Tuple2<Tuple2<String, String>, Iterable<String>>(key, friends);

							result.add(tuple2);
						}

						return result.iterator();
					}
				});

		// 步骤6 应用归约器

		JavaPairRDD<Tuple2<String, String>, Iterable<Iterable<String>>> groupByKey = pairs.groupByKey();

		for (Tuple2<Tuple2<String, String>, Iterable<Iterable<String>>> iterable_element : groupByKey.collect()) {
			System.out.println(iterable_element);
		}
		// ((100,600),[[200, 300, 400, 500, 600], []])
		// ((100,500),[[200, 300, 400, 500, 600], [100, 300]])
		// ((200,400),[[100, 300, 400], [100, 200, 300]])
		// ((200,300),[[100, 300, 400], [100, 200, 400, 500]])
		// ((300,400),[[100, 200, 400, 500], [100, 200, 300]])
		// ((100,200),[[200, 300, 400, 500, 600], [100, 300, 400]])
		// ((100,300),[[200, 300, 400, 500, 600], [100, 200, 400, 500]])
		// ((100,400),[[200, 300, 400, 500, 600], [100, 200, 300]])
		// ((300,500),[[100, 200, 400, 500], [100, 300]])
		// 步骤7 查找共同好友
		JavaPairRDD<Tuple2<String, String>, Iterable<String>> mapValues = groupByKey.mapValues(new Function<Iterable<Iterable<String>>, Iterable<String>>() {

			@Override
			public Iterable<String> call(Iterable<Iterable<String>> v1) throws Exception {
				// 此处只有2个集合，所有我直接求交集
				ArrayList<Iterable<String>> list = Lists.newArrayList(v1);
				ArrayList<String> aList = Lists.newArrayList(list.get(0));
				ArrayList<String> bList = Lists.newArrayList(list.get(1));
				aList.retainAll(bList);
				return aList;
			}
		});

		List<Tuple2<Tuple2<String, String>, Iterable<String>>> collect = mapValues.collect();
		for (Tuple2<Tuple2<String, String>, Iterable<String>> tuple2 : collect) {
			System.out.println(tuple2);
		}

		// ((100,600),[])
		// ((100,500),[300])
		// ((200,400),[100, 300])
		// ((200,300),[100, 400])
		// ((300,400),[100, 200])
		// ((100,200),[300, 400])
		// ((100,300),[200, 400, 500])
		// ((100,400),[200, 300])
		// ((300,500),[100])
	}

	/**
	 * 确保不会得到重复的对象
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	static Tuple2<String, String> buildSortPair(String a, String b) {
		return a.compareTo(b) > 0 ? new Tuple2<String, String>(b, a) : new Tuple2<String, String>(a, b);
	}
}
