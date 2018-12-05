package com.zhuzi.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.zhuzi.utils.SparkUtils;

import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

/**
 * @Title: JavaALSMLlib.java
 * @Package com.zhuzi.ml
 * @Description: TODO(官方 mllib包中的)
 * @author 作者 grq
 * @version 创建时间：2018年12月5日 下午9:50:57
 *
 */

public class JavaALSMLlib {

	static class ParseRating implements Function<String, Rating> {
		private static final long serialVersionUID = 1L;
		private static final Pattern COMMA = Pattern.compile("::");

		@Override
		public Rating call(String line) {
			String[] tok = COMMA.split(line);
			int x = Integer.parseInt(tok[0]);
			int y = Integer.parseInt(tok[1]);
			double rating = Double.parseDouble(tok[2]);
			return new Rating(x, y, rating);
		}
	}

	static class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String call(Tuple2<Object, double[]> element) {
			return element._1() + "," + Arrays.toString(element._2());
		}
	}

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("JavaALS").setMaster("local");
		int rank = 10;
		int iterations = 10;
		String outputDir = "E:/had/spark/out";
		int blocks = 1;

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(SparkUtils.getFilePath("data/mllib/als/sample_movielens_ratings.txt"));

		JavaRDD<Rating> ratings = lines.map(new ParseRating());

		MatrixFactorizationModel model = ALS.train(ratings.rdd(), rank, iterations, 0.01, blocks);

		model.userFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(outputDir + "/userFeatures");
		model.productFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(outputDir + "/productFeatures");
		System.out.println("Final user/product features written to " + outputDir);

		sc.stop();
	}
}
