package com.zhuzi.bookj.char08;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Title: ListDemo.java
 * @Package com.zhuzi.bookj.char08
 * @Description: 
 *               TODO(集合运算https://www.cnblogs.com/changfanchangle/p/8966860.html)
 * @author 作者 grq
 * @version 创建时间：2018年11月27日 下午9:09:11
 *
 */
public class ListDemo {
	public static void main(String[] args) {
		List<String> listA_01 = new ArrayList<String>(Arrays.asList("A", "B"));

		// 还能这样写 长知识了
		List<String> listB_01 = new ArrayList<String>() {
			{
				add("B");
				add("C");
			}
		};
		listA_01.retainAll(listB_01);
		System.out.println(listA_01); // 结果:[B]
		System.out.println(listB_01); // 结果:[B, C]

		String aA = "A";
		String bA = "B";

		System.out.println(bA.compareTo(aA));//-1
		String dA = "200";
		String d = "100";

		System.out.println(dA.compareTo(d));//1 
	}
}
