package com.zhuzi.utils;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @Title: Demo2.java
 * @Package com.zhuzi.utils
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年12月4日 下午2:10:34
 *
 */
public class Demo2 {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("JavaIndexToStringExample").master("local").getOrCreate();

		// $example on$
		List<Row> data = Arrays.asList(RowFactory.create(0, "a"), RowFactory.create(1, "b"), RowFactory.create(2, "c"), RowFactory.create(3, "a"), RowFactory.create(4, "a"), RowFactory.create(5, "c"));
		StructType schema = new StructType(new StructField[] { new StructField("id", DataTypes.IntegerType, false, Metadata.empty()), new StructField("category", DataTypes.StringType, false, Metadata.empty()) });
		Dataset<Row> df = spark.createDataFrame(data, schema);

		StringIndexerModel indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df);
		Dataset<Row> indexed = indexer.transform(df);

		System.out.println("Transformed string column '" + indexer.getInputCol() + "' " + "to indexed column '" + indexer.getOutputCol() + "'");
		indexed.show();

		StructField inputColSchema = indexed.schema().apply(indexer.getOutputCol());
		System.out.println("StringIndexer will store labels in output column metadata: " + Attribute.fromStructField(inputColSchema).toString() + "\n");

		IndexToString converter = new IndexToString().setInputCol("categoryIndex").setOutputCol("originalCategory");
		Dataset<Row> converted = converter.transform(indexed);

		System.out.println("Transformed indexed column '" + converter.getInputCol() + "' back to " + "original string column '" + converter.getOutputCol() + "' using labels in metadata");
		converted.select("id", "categoryIndex", "originalCategory").show();

		// $example off$
		spark.stop();
	}
}
