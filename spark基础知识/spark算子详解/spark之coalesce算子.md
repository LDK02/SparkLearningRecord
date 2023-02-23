# coalesce算子

**功能：**用于减少分区数量，Repartition也可以减少分区的数量，但repartition() 用于增加或减少 RDD、DataFrame、Dataset 分区，而 coalesce() 仅用于**有效减少分区**数量,也可以增加分区。

<font color=red>``coalesce`算子这是 `repartition()` 的优化或改进版本，其中使用合并的数据在分区之间的移动较低</font>。即使用合并的数据在分区之间的移动更少。

需要注意的重要一点是，Spark `repartition()` 和`coalesce()` 是**效率低下的操作**，因为它们会在许多分区中打乱数据，因此尽量减少重新分区。

我觉得源码中的关于coalesce的介绍，更加清楚：

> Return a new RDD that is reduced into numPartitions partitions.
> This results in a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of the 100 new partitions will claim 10 of the current partitions. If a larger number of partitions is requested, it will stay at the current number of partitions.
>
> However, if you're doing a drastic coalesce, e.g. to numPartitions = 1, this may result in your computation taking place on fewer nodes than you like (e.g. one node in the case of numPartitions = 1). To avoid this, you can pass shuffle = true. This will add a shuffle step, but means the current upstream partitions will be executed in parallel (per whatever the current partitioning is).
>
> 如果您正在进行剧烈合并，例如numPartitions=1，这可能会导致您的计算发生在比您希望的更少的节点上（例如，在numPartitions＝1的情况下，一个节点）。为了避免这种情况，可以传递shuffle=true。这将添加一个shuffle步骤，但意味着当前上游分区将并行执行（无论当前分区是什么）。
>
> Note:
> With shuffle = true, you can actually coalesce to a larger number of partitions. This is useful if you have a small number of partitions, say 100, potentially with a few partitions being abnormally large. Calling coalesce(1000, shuffle = true) will result in 1000 partitions with the data distributed using a hash partitioner. The optional partition coalescer passed in must be serializable.

# 案例

## Spark RDD coalesce()

下面在创建RDD时，我们分别以默认的方式创建RDD，以及指定分区数的方式创建RDD，并分别打印其具体的分区数值。并使用coalesce算子对创建的RDD重新分区，并打印分区数，具体代码：

```scala
package scalaNote.spark.RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import java.lang

object coalesceDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("coalesceDemo")
      .master("local[5]")
      .getOrCreate()
    /**1 RDD repartition coalesce */
    //创建RDD，
    val rdd: RDD[Int] = spark.sparkContext.parallelize(Range(0, 20))
    println("默认分区数为：" + rdd.partitions.size)//注意分区数和local[5]中有关系

    //创建RDD并指定分区数
    val rdd1: RDD[Int] = spark.sparkContext.parallelize(Range(0, 25), 6)
    println("指定分区数：" + rdd1.partitions.size)

    //减少rdd1的分区数量
    val coalescerdd = rdd1.coalesce(4)
    println("减少后的分区数：" + coalescerdd.partitions.size)

    //coalesce也可以增加分区数，但
    println("使用coalescer增加分区数："+rdd1.coalesce(8).partitions.size)
  }

}

```

## **Spark DataFrame repartition() **

```scala
package scalaNote.spark.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import java.lang

object coalesceDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("coalesceDemo")
      .master("local[5]")
      .getOrCreate()

    /**
      *
      * 2 dataframe repartition coalesce
      * */

    val df: Dataset[lang.Long] = spark.range(0, 20)
    //分区数和lcocal[5]设置的参数一致
    println(df.rdd.partitions.length)//5

    /** 2.1 dataframe repartition */
    val df2: Dataset[lang.Long] = df.repartition(6)
    println(df2.rdd.partitions.length)//6

    /** dataframe coalesce */
    val df3: Dataset[lang.Long] = df.coalesce(2)
    println(df3.rdd.partitions.length)//2
  }

}
```
