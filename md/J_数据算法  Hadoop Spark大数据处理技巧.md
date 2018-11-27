#



# 8 共同好友

 	找出共同好友，此处提供3种解决方案：

- MapReduce/Hadoop解决方案，使用基本数据类型
- MapReduce/Hadoop解决方案，使用定制数据类型
- Spark解决方案，使用RDD



```shell
100, 200 300 400 500 600
200, 100 300 400
300, 100 200 400 500
400, 100 200 300
500, 100 300
600, 100
```

> 数据说明，用户500 有2个好友，分别是用户100 和用户300。 用户600只有一个用户100的好友



### Spark方案

----

​	此处使用Spark的RDD编写map和reduce函数，用户的数据从文本中读取，格式是(P是某个人,{F1,F2,F...})是P的直接好友

​	

解决步骤如下：

1. 导入必要的接口
2. 检查输入数据
3. 创建一个JavaSparkContext对象
4. 创建第一个javaRDD表示输入文件
5. 将JavaRDD(String)映射到键值对，其中key=Tuple<u1,u2> ,value=好友列表
6. 将(key=Tuple2<u1,u2>,value=List(friends))对归约为（key=Tuple2<u1,u2>,value=List<List(friends)>）
7. 利用所有的List<List<Long>>的交集查找共同好友

