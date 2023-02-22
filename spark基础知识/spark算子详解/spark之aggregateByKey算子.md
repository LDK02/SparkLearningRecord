# aggregateByKey算子

aggregateByKey和aggregate功能一致，主要用于统计，他俩之间区别在于aggregateByKey是依据key进行汇总的。

**功能：**根据设定规则同时进行分区间的计算和分区内的计算，具体为，（1）在分区内按照相同的key进行某种计算，分区内部计算完后，接着计算分区间的（2）同样依据相同的key按照规则进行计算。

**注意：**数据类型为键值对类型，aggregateByKey函数有三个参数列表，即：aggregateByKey(参数)(参数1，参数2)，其中：

- 参数：在进行分区内计算时，依据分区内的计算规则，设置一个初始值，进行第一步的计算。
- 参数1：分区内的计算规则
- 参数2：分区间的计算规则

# 图解计算过程

![image-20230222195637342](D:\Typora\文档\spark学习笔记\spark学习笔记记录\spark基础知识\spark算子详解\spark之aggregateByKey算子.assets\image-20230222195637342.png)

这里以将相同key的value和为例，解释aggregatebykey为计算过程：

1 首先给定初始值0，每个分区都会使用初始值。

2 在各个分区中，会按照key为分割点，将相同的key放在一起，这里我们定义对value的操作为求和（实际应用中可以将求和的操作换做其他逻辑），分区计算完成后，会带着返回key一起返回。

3 分区内部计算完成后，就开始汇总各个分区间的计算结果，任然是以相同的key为基准，将相同key的value进行求和，并带着key一起返回最终结果。

```scala
package scalaNote.spark.RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object aggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a", 1), ("b", 2),("b", 3), ("a", 4), ("c", 6),("a", 6)), 2)
    /**
      * 案例1 将k相等的value相加
      * */
    def param1 = (accu: Int, v: Int) => accu + v
    //分区内的计算规则
    def param2 = (accu1: Int, accu2: Int) => accu1 + accu2
    //分区间的计算规则
    val wordCount2: RDD[(String, Int)] = rdd.aggregateByKey(0)(param1, param2)
    print(wordCount2.collect().foreach(println))
    println("--------------------------------")
    //简化版本,并这里将初始值设置为1
    rdd.aggregateByKey(1)(
      (x, y) => x+y,//分区内部计算规则，x和y可以理解为k->v中的value，值
      (x, y) => x + y//分区间计算规则，x,y可以理解为分区1最终计算结果，分区2最终计算结果，
    ).collect().foreach(println)


    sc.stop()
  }

}
```

```scala
结果：
(b,5)
(a,11)
(c,6)
()--------------------------------
(b,6)
(a,13)
(c,7)
```



# 案例

## **实例1**

```scala
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2),("a", 4),("a", 6)),2)

    rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      (x,y) => x  + y
    ).collect().foreach(println)


    sc.stop()
  }

}
结果：
(a,8)
```

**计算步骤：**

分区1为：("a", 1), ("a", 2)，分区2为：("a", 4),("a", 6)

1. 分区内计算：相同key的a的最大值为2，相同的key的b的最大值为6，
2. 分区间计算：相同key为a,进行区间的计算，返回结果2+6=8。

## **实例2**

```scala
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2),("b", 4),("b", 6)),2)

    rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      (x,y) => x  + y
    ).collect().foreach(println)


    sc.stop()
  }

}
结果：
(b,6)
(a,2)
```

**计算步骤：**

分区1为：("a", 1), ("a", 2)，分区2为：("b", 4),("b", 6)

1. 分区内计算：相同key的a的最大值为2，相同的key的b的最大值为6，
2. 分区间计算：由于没有相同的key,则不进行计算，返回结果(b,6) (a,2)。

## **特殊情况：**

分区内和分区间的计算规则一样时，可以进行简写。

```scala
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey_3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2),("a", 4),("a", 6)),2)

    rdd.aggregateByKey(0)(
      (x,y) => x + y,
      (x,y) => x  + y
    ).collect().foreach(println)
    //上面的简写
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)

    sc.stop()
  }

}
结果：
(a,13)
(a,13)
```

## 进阶：

使用aggregatebykey获取相同key的数据的平均值。

```scala
package scalaNote.spark.RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object aggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a", 1), ("b", 2),("b", 3), ("a", 4), ("c", 6),("a", 6)), 2)

    /**进阶案例
      * 求相同key的平均值。
      * 既然要求key的平均值，那么就需要知道相同key的个数，以及和，
      * 故需要保存两个变量一个保存和，一个保存个数，
      * 故可以将初始值设为（0,0）
      * */
    //分区内计算方式
    def op(t:(Int,Int),v:Int)={
      //t是初始值，v是RDD中的元素，且为value值的类型
      (t._1 + v, t._2 + 1)//求相同key(求和，统计个数)
    }
    //分区间的计算方式
    def com(t1:(Int,Int),t2:(Int,Int))={
      (t1._1 + t2._1, t1._2 + t2._2)
    }

    val res: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(op, com)
    val resultRDD: RDD[(String, Int)] = res.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    resultRDD.collect().foreach(println)
    println("--------简化方式结果：----------")
    //简化方式
    rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    ).mapValues{
      case (num, cnt) => {
        num / cnt
      }
    }.collect().foreach(println)

    sc.stop()
  }

}

```

# 总结：

aggregateByKey在使用过程中，和普通编程过程不一致的地方在于：编程过程中不要考虑对key的操作，对的key的操作spark会自动按照类别处理，编程过程中只需把关注点放在value和初始值上就可以了，

