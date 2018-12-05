package com.zhuzi.ml;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhuzi.utils.SparkUtils;

// $example off$

/**
 * @Title: JavaALS.java
 * @Package com.zhuzi.ml
 * @Description: TODO(LASdemo)
 * @author 作者 grq
 * @version 创建时间：2018年12月5日 下午7:30:59
 *          https://www.cnblogs.com/mstk/p/7208674.html
 */
public class JavaALS {
	private static Logger log = LoggerFactory.getLogger(JavaALS.class);

	public static class Rating implements Serializable {

		private static final long serialVersionUID = 1L;
		private int userId;
		private int movieId;
		private float rating;
		private long timestamp;

		public Rating() {
		}

		public Rating(int userId, int movieId, float rating, long timestamp) {
			this.userId = userId;
			this.movieId = movieId;
			this.rating = rating;
			this.timestamp = timestamp;
		}

		public int getUserId() {
			return userId;
		}

		public int getMovieId() {
			return movieId;
		}

		public float getRating() {
			return rating;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public static Rating parseRating(String str) {
			String[] fields = str.split("::");
			if (fields.length != 4) {
				throw new IllegalArgumentException("每行必须是 4 fields");
			}
			int userId = Integer.parseInt(fields[0]);
			int movieId = Integer.parseInt(fields[1]);
			float rating = Float.parseFloat(fields[2]);
			long timestamp = Long.parseLong(fields[3]);
			return new Rating(userId, movieId, rating, timestamp);
		}
	}

	public static void main(String[] args) {
		SparkSession spark = SparkUtils.buildSparkSession();
		JavaRDD<Rating> ratingsRDD = spark.read().textFile(SparkUtils.getFilePath("data/mllib/als/sample_movielens_ratings.txt")).javaRDD().map(Rating::parseRating);
		ratingsRDD.cache();
		Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
		Dataset<Row>[] splits = ratings.randomSplit(new double[] { 0.8, 0.2 });
		Dataset<Row> training = splits[0];
		Dataset<Row> test = splits[1];

		// 利用训练数据建立ALS推荐模型
		ALS als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating");
		ALSModel model = als.fit(training);

		// Evaluate the model by computing the RMSE on the test data Note we set
		// cold start strategy to 'drop' to ensure we don't get NaN evaluation
		// metrics
		model.setColdStartStrategy("drop");
		Dataset<Row> predictions = model.transform(test);

		RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction");
		Double rmse = evaluator.evaluate(predictions);
		System.out.println("Root-mean-square error = " + rmse);

		// 为每个用户生成前10个电影推荐
		Dataset<Row> userRecs = model.recommendForAllUsers(5);
		userRecs.cache();
		System.out.println(userRecs.count());

		for (Row dataset : userRecs.collectAsList()) {
			log.warn(dataset.toString());
		}
		log.warn("以上是：为每个用户生成前10个电影推荐");
		// ////////////////////////////////////////////////////////////

		// 为每部电影生成十大用户推荐
		Dataset<Row> movieRecs = model.recommendForAllItems(5);
		movieRecs.cache();
		long count = movieRecs.count();
		System.out.println(count);

		for (Row dataset : movieRecs.collectAsList()) {
			log.warn(dataset.toString());
		}
		log.warn("以上是：为每部电影生成十大用户推荐");
		// 为指定的一组用户生成前10个电影推荐
		Dataset<Row> users = ratings.select(als.getUserCol()).distinct().limit(3);

		Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users, 10);

		// 为指定的电影集生成前10个用户推荐
		Dataset<Row> movies = ratings.select(als.getItemCol()).distinct().limit(3);
		Dataset<Row> movieSubSetRecs = model.recommendForItemSubset(movies, 10);

	}
}
