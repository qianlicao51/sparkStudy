package com.zhuzi.utils;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * @Title: VectoDemo.java
 * @Package com.zhuzi.utils
 * @Description: TODO(java 创建向量)
 * @author 作者 grq
 * @version 创建时间：2018年12月5日 下午3:57:40
 *
 */
public class VectoDemo {
	public static void main(String[] args) {

		// 创建稠密向量<1.0,2.0,3.0>;
		Vector dense = Vectors.dense(1.0, 2.0, 3.0);
		//
		Vector dense2 = Vectors.dense(new double[] { 1.0, 2.0, 3.0 });

		// 创建稀疏向量<1.0,0.0,2.0,0.0>
		Vector sparse = Vectors.sparse(4, new int[] { 0, 2 }, new double[] { 1.0, 2.0 });

	}

}
