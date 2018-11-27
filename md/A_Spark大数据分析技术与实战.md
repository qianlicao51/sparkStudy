# Spark大数据分析技术与实战

## 楔子

> 之前了解的一些内容，此处直接从RDD编程开始学习。



# 3 RDD编程

​	Spark是基于内存的大数据综合处理框架，为大数据处理提供了一个一体化解决方案，而该方案的设计与实现都是基于一个核心概念展开的，即弹性分布式数据集（Resilient Distributed Dataset，RDD）。RDD可以理解为由若干个元素构成的分布式集合以及其上的操作，它是Spark中数据的重要组织形式。与MapReduce不同，Spark针对RDD提供了更加丰富的操作，而不只局限于Map和Reduce，用户利用这些操作可以非常方便地编写出复杂的业务逻辑，然后Spark会自动将RDD中的数据与相关任务分发到集群上，并行化地去执行。



## 3.1 RDD定义

​	Apache将RDD定义为弹性分布式数据集，它是Spark应用程序中数据的基本组织形式。弹性意味着RDD能够自动地进行内存和磁盘数据存储的切换，并且具有非常高的容错性；分布式说明RDD是一个存储在多个节点上的海量数据集合。RDD是一种高度受限的共享内存模型，即RDD是只读的记录分区的集合。RDD具有自动容错、位置感知调度和可伸缩性等数据流模型的特点。

​	

## 3.2 RDD的特性

​	Spark在定义和描述RDD的时候，通常会涉及以下五个接口

![](A_Spark大数据/3.2.1.png)

### 3.2.1分区

 	RDD中的数据可能是TB、PB级别的，完全基于一个节点上的存储与计算并不现实。Spark基于分布式的思想，先将RDD划分为若干子集，每个子集称为一个分区（partition）。分区是RDD的基本组成单位，与MapReduce中的split类似。对于一个RDD，Spark以分区为单位逐个计算分区中的元素。分区的多少决定了这个RDD进行并行计算的粒度，因为对RDD中每一个分区的计算都是在一个单独的任务中执行的。用户可以显示指定RDD的分区数目，若不指定Spark将会采用默认值（即CPU核数）。

```java
JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext();
// 指定RDD分区数量 4个分区
sparkContext.parallelize(Arrays.asList("hello", "spark"), 4);

```

