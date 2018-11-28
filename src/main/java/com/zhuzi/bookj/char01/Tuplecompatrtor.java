package com.zhuzi.bookj.char01;

import java.util.Comparator;

import scala.Tuple2;

/**
 * @Title: Tuplecompatrtor.java
 * @Package com.zhuzi.bookj.char01
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年11月28日 下午3:10:42
 *
 */
public class Tuplecompatrtor implements Comparator<Tuple2<String, String>> {

	@Override
	public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {

		return o1._1.compareTo(o2._1);
	}
}
