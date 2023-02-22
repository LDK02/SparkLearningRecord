# aggregate算子

aggregate的字面意思是：`总计`,其字面意思暗含了其具体用法，即`汇总`。

**功能：**按照规则同时进行分区内的计算和分区间的计算。

**源码：**

```scala
* @param zeroValue 每个分区的累积结果的初始值，
    * @param seqOp 用于在分区内累积结果的运算符
    * @param combOp 一个关联运算符，用于组合来自不同分区的结果
    */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = withScope 
```

有三个参数：

- 初始值：zeroValue
- 分区内的计算规则:seqOp
- 分区间的计算规则:combOp

分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合，具体操作过程如下图所示：（**注意：**初始值使用了两次）

## 图解计算过程

![image-20220328160728002](D:\Typora\文档\spark学习笔记\spark学习笔记记录\spark基础知识\spark算子详解\spark之aggregate()算子.assets\image-20220328160728002.png)

假设有一个RDD，这个RDD有两个分区，给定初始值０，使用aggregate算子进行计算时，首先在每个分区内调用函数seqop进行计算，在分区1最终计算结果为4，分区2最终计算结果为6，最后调用comop函数合并每个分区的计算结果。

# 案例

```scala
package scalaNote.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object aggregateDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("aggregateDemo")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    /** 准备数据   */
    val inputRDD: RDD[(String, Int)] =
      spark.sparkContext
        .parallelize(
          List(("Z", 1),
            ("A", 20),
            ("B", 30),
            ("C", 40),
            ("B", 30),
            ("B", 60)))


    val listRdd: RDD[Int] = spark.
      sparkContext.
      parallelize(List(1, 2, 3, 4, 5, 3, 2))

    /** aggregate算子   */
      /**
        * 案例1 求 （1, 2, 3, 4, 5, 3, 2)的和
        * */
    //定义分区内的计算方式，可以认为s1是初始值，s2是分区内的第一个元素
    def seqOp(s1: Int, s2: Int): Int = {
      println("seq计算时的输入值: " + s1 + ":" + s2)
      s1 + s2
    }
    //定义分区间的和，c1是一个分区求和后的值，c2是另一个分区求和后的值
    def combOp(c1: Int, c2: Int): Int = {
      println("comb的计算时的输入值: " + c1 + ":" + c2)
      c1 + c2
    }
    //这里设置初始值为0
    val aggResult0: Int = listRdd.aggregate(0)(seqOp, combOp)
    println("案例1 aggreagate计算方式1结果" + aggResult0)

    /** 可以将上函数进行简化*/
    //分区内的函数seqOP和param0等价，分区间函数combOp和param1等价，函数简写形式
    def param0 = (accu: Int, v: Int) => accu + v

    def param1 = (accu1: Int, accu2: Int) => accu1 + accu2

    val aggResult: Int = listRdd.aggregate(0)(param0, param1)
    println("案例1 aggreagate计算方式2结果" + aggResult)

    //案例2
    /**方式1*/
    //定义分区内的计算方式，可以认为s1是初始值，s2是分区内的第一个元素,元素的类型和定义时是一致的
    def seqOp1(s1: Int, s2:(String, Int)): Int = {
      println("seq计算时的输入值: " + s1 + ":" + s2)
      s1 + s2._2
    }

    //定义分区间的和，c1是一个分区求和后的值，c2是另一个分区求和后的值
    def combOp1(c1: Int, c2: Int): Int = {
      println("comb的计算时的输入值: " + c1 + ":" + c2)
      c1 + c2
    }
    val reAgg: Int = inputRDD.aggregate(0)(seqOp1, combOp1)
    println("案例2 aggreagate计算方式1 结果" + reAgg)
    /**方式2简写*/
    def param3 = (accu: Int, v: (String, Int)) => accu + v._2
    def param4 = (accu1: Int, accu2: Int) => accu1 + accu2
    val reAgg1: Int = inputRDD.aggregate(0)(param3, param4)
    println("案例2 aggreagate计算方式2 结果" + reAgg1)


  }

}

```



