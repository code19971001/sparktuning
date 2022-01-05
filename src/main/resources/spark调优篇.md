## Spark 调优

### 1 前提

1.搭建好的hadoop集群，hive，还有spark。

> hadoop为2nn模式且搭建好了kerberos认证和ranger授权。

|        | hadoop02      | hadoop03 | hadoop04 |
| ------ | ------------- | -------- | -------- |
| hadoop | √             | √        | √        |
| hive   | √             |          |          |
| spark  | √(调度为yarn) |          |          |

2.准备测试数据

![image-20220102155723793](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220102155723793.png)

3.导入数据到hive表中

```scala
object InitUtil {
    
  def main(args: Array[String]): Unit = {
    //主要用于kerberos的认证
    try {

      //等同于把krb5.conf放在$JAVA_HOME\jre\lib\security，一般写代码即可
      System.setProperty("java.security.krb5.conf", "C:\\ProgramData\\MIT\\Kerberos5\\krb5.ini")
      //下面的conf可以注释掉是因为在core-site.xml里有相关的配置，如果没有相关的配置，则下面的代码是必须的
      //      val conf = new Configuration
      //      conf.set("hadoop.security.authentication", "kerberos")
      //      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab("code1997@CODE1997.COM", "C:\\ProgramData\\MIT\\Kerberos5\\code1997.keytab")
      println(UserGroupInformation.getCurrentUser, UserGroupInformation.getLoginUser)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    val sparkConf = new SparkConf().setAppName("InitData")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = initSparkSession(sparkConf)
    initHiveTable(sparkSession)
    //initBucketTable(sparkSession)
    saveData(sparkSession)
  }

  def initSparkSession(sparkConf: SparkConf): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "code1997")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    // TODO 改成自己的hadoop的nameNode的url，在core-site文件中
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop02:8020")
    sparkSession
  }

  def initHiveTable(sparkSession: SparkSession): Unit = {
    sparkSession.read.json("/origin_data/sparktuning/coursepay.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_pay")

    sparkSession.read.json("/origin_data/sparktuning/salecourse.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.sale_course")

    sparkSession.read.json("/origin_data/sparktuning/courseshoppingcart.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_shopping_cart")

  }

  def initBucketTable(sparkSession: SparkSession): Unit = {
    sparkSession.read.json("/origin_data/sparktuning/coursepay.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .bucketBy(5, "orderid")
      .sortBy("orderid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_pay_cluster")
    sparkSession.read.json("/origin_data/sparktuning/courseshoppingcart.log")
      .write.partitionBy("dt", "dn")
      .bucketBy(5, "orderid")
      .format("parquet")
      .sortBy("orderid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_shopping_cart_cluster")
  }

  def saveData(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => Student(item, "name" + item, random.nextInt(100), random.nextInt(100)))
    }).write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("sparktuning.test_student")

    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => School(item, "school" + item, random.nextInt(100)))
    }).write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("sparktuning.test_school")
  }
}
```

### 2 explain

> explain可以用来查看spark sql的执行计划。

#### 2.1 基本语法

```scala
.explain(mode='xxx')
```

- mode="simple"：只展示物理执行计划。
- mode="extended"：展示物理执行计划和逻辑执行计划。
- mode="codegen"：展示要 Codegen 生成的可执行 Java 代码。
- mode="cost"：展示优化后的逻辑执行计划以及相关的统计。
- mode="formatted"：以分隔的方式输出，它会输出更易读的物理执行计划，并展示每个节点的详细信息。

#### 2.2 执行计划处理流程

![image-20220102160231794](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220102160231794.png)

code：一般情况下，我们分析的是物理执行执行计划。

```scala
object ExplainDemo {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ExplainDemo")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

    val sqlStr =
      """
        |select
        |  sc.courseid,
        |  sc.coursename,
        |  sum(sellmoney) as totalsell
        |from sale_course sc join course_shopping_cart csc
        |  on sc.courseid=csc.courseid and sc.dt=csc.dt and sc.dn=csc.dn
        |group by sc.courseid,sc.coursename
      """.stripMargin


    sparkSession.sql("use sparktuning;")
    //    sparkSession.sql(sqlStr).show()
    //    while(true){}

    println("=====================================explain()-只展示物理执行计划============================================")
    sparkSession.sql(sqlStr).explain()

    println("===============================explain(mode = \"simple\")-只展示物理执行计划=================================")
    sparkSession.sql(sqlStr).explain(mode = "simple")

    println("============================explain(mode = \"extended\")-展示逻辑和物理执行计划==============================")
    sparkSession.sql(sqlStr).explain(mode = "extended")

    println("============================explain(mode = \"codegen\")-展示可执行java代码===================================")
    sparkSession.sql(sqlStr).explain(mode = "codegen")

    println("============================explain(mode = \"formatted\")-展示格式化的物理执行计划=============================")
    sparkSession.sql(sqlStr).explain(mode = "formatted")
    
  }
}
```

执行计划的展示：

```txt
============================explain(mode = "extended")-展示逻辑和物理执行计划==============================
== Parsed Logical Plan == 只是校验语法
'Aggregate ['sc.courseid, 'sc.coursename], ['sc.courseid, 'sc.coursename, 'sum('sellmoney) AS totalsell#38]
+- 'Join Inner, ((('sc.courseid = 'csc.courseid) AND ('sc.dt = 'csc.dt)) AND ('sc.dn = 'csc.dn))
   :- 'SubqueryAlias sc
   :  +- 'UnresolvedRelation [sale_course]
   +- 'SubqueryAlias csc
      +- 'UnresolvedRelation [course_shopping_cart]

== Analyzed Logical Plan == 校验语法的基础上添加hive的元数据信息校验
courseid: bigint, coursename: string, totalsell: double
Aggregate [courseid#3L, coursename#5], [courseid#3L, coursename#5, sum(cast(sellmoney#22 as double)) AS totalsell#38]
+- Join Inner, (((courseid#3L = courseid#17L) AND (dt#15 = dt#23)) AND (dn#16 = dn#24))
   :- SubqueryAlias sc
   :  +- SubqueryAlias spark_catalog.sparktuning.sale_course
   :     +- Relation[chapterid#1L,chaptername#2,courseid#3L,coursemanager#4,coursename#5,edusubjectid#6L,edusubjectname#7,majorid#8L,majorname#9,money#10,pointlistid#11L,status#12,teacherid#13L,teachername#14,dt#15,dn#16] parquet
   +- SubqueryAlias csc
      +- SubqueryAlias spark_catalog.sparktuning.course_shopping_cart
         +- Relation[courseid#17L,coursename#18,createtime#19,discount#20,orderid#21,sellmoney#22,dt#23,dn#24] parquet

== Optimized Logical Plan == 基于rbo的方式，对逻辑执行计划进行优化，比如：谓词下推，列裁剪，常量替换
Aggregate [courseid#3L, coursename#5], [courseid#3L, coursename#5, sum(cast(sellmoney#22 as double)) AS totalsell#38]
+- Project [courseid#3L, coursename#5, sellmoney#22]
   +- Join Inner, (((courseid#3L = courseid#17L) AND (dt#15 = dt#23)) AND (dn#16 = dn#24))
      :- Project [courseid#3L, coursename#5, dt#15, dn#16]
      :  +- Filter ((isnotnull(dt#15) AND isnotnull(courseid#3L)) AND isnotnull(dn#16))
      :     +- Relation[chapterid#1L,chaptername#2,courseid#3L,coursemanager#4,coursename#5,edusubjectid#6L,edusubjectname#7,majorid#8L,majorname#9,money#10,pointlistid#11L,status#12,teacherid#13L,teachername#14,dt#15,dn#16] parquet
      +- Project [courseid#17L, sellmoney#22, dt#23, dn#24]
         +- Filter ((isnotnull(dt#23) AND isnotnull(courseid#17L)) AND isnotnull(dn#24))
            +- Relation[courseid#17L,coursename#18,createtime#19,discount#20,orderid#21,sellmoney#22,dt#23,dn#24] parquet

== Physical Plan == (num)代表执行顺序，如果缩进量一样就代表是并行执行的。
聚合
*(3) HashAggregate(keys=[courseid#3L, coursename#5], functions=[sum(cast(sellmoney#22 as double))], output=[courseid#3L, coursename#5, totalsell#38])
	执行shuffle
+- Exchange hashpartitioning(courseid#3L, coursename#5, 200), true, [id=#203]
		分区内的预聚合
   +- *(2) HashAggregate(keys=[courseid#3L, coursename#5], functions=[partial_sum(cast(sellmoney#22 as double))], output=[courseid#3L, coursename#5, sum#44])
      +- *(2) Project [courseid#3L, coursename#5, sellmoney#22]
         +- *(2) BroadcastHashJoin [courseid#3L, dt#15, dn#16], [courseid#17L, dt#23, dn#24], Inner, BuildLeft
            :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true], input[2, string, true], input[3, string, true])), [id=#193]
            :  +- *(1) Project [courseid#3L, coursename#5, dt#15, dn#16]
            :     +- *(1) Filter isnotnull(courseid#3L)
            :        +- *(1) ColumnarToRow
            :           +- FileScan parquet sparktuning.sale_course[courseid#3L,coursename#5,dt#15,dn#16] Batched: true, DataFilters: [isnotnull(courseid#3L)], Format: Parquet, Location: InMemoryFileIndex[hdfs://hadoop02:8020/user/hive/warehouse/sparktuning.db/sale_course/dt=20190722..., PartitionFilters: [isnotnull(dt#15), isnotnull(dn#16)], PushedFilters: [IsNotNull(courseid)], ReadSchema: struct<courseid:bigint,coursename:string>
            +- *(2) Project [courseid#17L, sellmoney#22, dt#23, dn#24]
               +- *(2) Filter isnotnull(courseid#17L)
                  +- *(2) ColumnarToRow
                     +- FileScan parquet sparktuning.course_shopping_cart[courseid#17L,sellmoney#22,dt#23,dn#24] Batched: true, DataFilters: [isnotnull(courseid#17L)], Format: Parquet, Location: InMemoryFileIndex[hdfs://hadoop02:8020/user/hive/warehouse/sparktuning.db/course_shopping_cart/dt..., PartitionFilters: [isnotnull(dt#23), isnotnull(dn#24)], PushedFilters: [IsNotNull(courseid)], ReadSchema: struct<courseid:bigint,sellmoney:string>
```

1. Unresolved 逻辑执行计划：== Parsed Logical Plan ==
   - Parser 组件检查 SQL 语法上是否有问题，然后生成 Unresolved（未决断）的逻辑计划，
     不检查表名、不检查列名。
2. Resolved 逻辑执行计划：== Analyzed Logical Plan ==
   - 通过访问 Spark 中的 Catalog 存储库来解析验证语义、列名、类型、表名等。
3. 优化后的逻辑执行计划：== Optimized Logical Plan ==
   - Catalyst 优化器根据各种规则进行优化。RBO(rules base Optimized)
4. 物理执行计划：== Physical Plan ==
   - HashAggregate 运算符表示数据聚合，一般 HashAggregate 是成对出现，第一个HashAggregate 是将执行节点本地的数据进行局部聚合，另一个 HashAggregate 是将各个分区的数据进一步进行聚合计算。
   - Exchange 运算符其实就是 shuffle，表示需要在集群上移动数据。很多时候HashAggregate 会以 Exchange 分隔开来。
   - Project 运算符是 SQL 中的投影操作，就是选择列(列裁剪)（例如：select name, age…）。
   - BroadcastHashJoin 运算符表示通过基于广播方式进行 HashJoin。
   - LocalTableScan 运算符就是全表扫描本地的表。

web ui方式：实际工作过程中，我们一般通过web ui来查看执行计划。

![image-20220102170657475](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220102170657475.png)

### 3 资源调优

#### 3.1 服务器资源

##### 3.1.1 总体考虑

以单台服务器 128G 内存，32 线程为例。
先设定单个 Executor 核数，根据 Yarn 配置得出每个节点最多的 Executor 数量，每个节点的 yarn 内存/每个节点数量=单个节点的数量。那么总的 executor 数=单节点数量*节点数。

2.具体的提交参数

1）executor-cores
每个 executor 的最大核数。根据经验实践，设定在 3~6 之间比较合理。
2）num-executors
该参数值=每个节点的 executor 数 * work 节点数
每个 node 的 executor 数 = 单节点 yarn 总核数 / 每个 executor 的最大 cpu 核数
考虑到系统基础服务和 HDFS 等组件的余量，yarn.nodemanager.resource.cpu-vcores 配置为：28，参考executor-cores 的值为：4，那么每个 node 的 executor 数 = 28/4 = 7,假设集群节点为 10，那么 num-executors = 7 * 10 = 70
3）executor-memory：粗略估算
该参数值=yarn-nodemanager.resource.memory-mb / 每个节点的 executor 数量
如果 yarn 的参数配置为 100G，那么每个 Executor 大概就是 100G/7≈14G,同时要注意yarn 配置中每个容器允许的最大内存是否匹配。

3.实际

|        | hadoop02 | hadoop03 | hadoop04 |
| ------ | -------- | -------- | -------- |
| vcore  | 2        | 1        | 1        |
| memory | 5        | 3        | 3        |

##### 3.1.2 内存估算

内存分布模型：

![image-20220102174224892](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220102174224892.png)

- Other内存=自定义数据结构*每个Executor核数。
- Storage内存=广播变量+cache/executor数量。
- Executor内存=每个Excutor核数*(数据集大小/并行度)，比如shuffle，reduceByKey,GroupByKey，对于sparksql来说，默认的shuffle的并行度为200

##### 2.1.3 调整内存配置项

一般情况下，各个区域的内存比例保持默认值即可。

spark.memory.fraction=（估算 storage 内存+估算 Execution 内存）/（估算 storage 内存+估算 Execution 内存+估算 Other 内存）

spark.memory.storageFraction =（估算 storage 内存）/（估算 storage 内存+估算Execution 内存）

代入公式：

Storage 堆内内存=(spark.executor.memory–300MB)*spark.memory.fraction*spark.memory.storageFraction，如果不够会落盘。

Execution 堆内内存=(spark.executor.memory–300MB)*spark.memory.fraction*(1-spark.memory.storageFraction)，如果不够会直接挂掉。

#### 3.2 持久化和序列化

##### 3.2.1 RDD

1.cache

source code:默认的情况下是基于内存的。

```scala
  /**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
   */
  def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

  /**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
   */
  def cache(): this.type = persist()
```

demo code：

```scala
  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf().setAppName("RddCacheDemo")
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    val result = sparkSession.sql("select * from sparktuning.course_pay").rdd
    result.cache()
    result.foreachPartition(( p: Iterator[Row] ) => p.foreach(item => println(item.get(0))))
    while (true) {
      //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让spark context立马结束
    }
  }
```

提交代码：需要注意自己的虚拟机的参数信息

```shell
./spark-submit --master yarn --deploy-mode client --driver-memory 1g --num-executors 3 --executor-cores 1 --executor-memory 2g --class com.atguigu.sparktuning.cache.RddCacheDemo --jars spark-tuning-1.0-SNAPSHOT-jar-with-dependencies.jar
```

![image-20220103223846829](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220103223846829.png)

2.cache and kyro ser

demo code:

```scala
def main(args: Array[String]): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("RddCacheKryoDemo")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[CoursePay]))


  val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

  import sparkSession.implicits._
  val result = sparkSession.sql("select * from sparktuning.course_pay").as[CoursePay].rdd
  result.persist(StorageLevel.MEMORY_ONLY_SER)
  result.foreachPartition(( p: Iterator[CoursePay] ) => p.foreach(item => println(item.orderid)))

  while (true) {
    //因为历史服务器上看不到，storage内存占用，所以这里加个死循环 不让sparkcontext立马结束
  }
}
```

效果对比

![image-20220103224334527](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220103224334527.png)

##### 3.2.2 Dataset

source code:默认的缓存级别是**内存和磁盘**，如果内存不够的情况下会缓存到磁盘中。

```scala
  def cacheQuery(
      query: Dataset[_],
      tableName: Option[String] = None,
      storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = 


/**
 * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
 *
 * @group basic
 * @since 1.6.0
 */
def persist(): this.type = {
  sparkSession.sharedState.cacheManager.cacheQuery(this)
  this
}

/**
 * Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
 *
 * @group basic
 * @since 1.6.0
 */
def cache(): this.type = persist()
```

demo code:

```scala
  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf().setAppName("DataSetCacheDemo")
//      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)


    import sparkSession.implicits._
    val result = sparkSession.sql("select * from sparktuning.course_pay").as[CoursePay]
    result.cache()
    result.foreachPartition(( p: Iterator[CoursePay] ) => p.foreach(item => println(item.orderid)))
    while (true) {
    }

  }
```

效果图：上为MEMORY_AND_DISK，下为MEMORY_AND_DISK_SER。

spark sql而言存在encoder，实现了自己的序列化方式，因此不需要我们自己来注册序列化方式。

#### ![image-20220103225102231](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220103225102231.png)3.3 CPU调优

##### 3.3.1 并行度和并发度

1）并行度

- spark.default.parallelism：设置 RDD 的默认并行度，没有设置时，由 join、reduceByKey 和 parallelize 等转换决定，不能设置spark sql的数据结构的并行度。
- spark.sql.shuffle.partitions：
- 适用 SparkSQL 时，Shuffle Reduce 阶段默认的并行度，默认 200。此参数只能控制Spark sql、DataFrame、DataSet 分区个数。不能控制 RDD 分区个数

2）并发度

同时执行的task数。

##### 3.3.2 cpu低效的原因

Executor 接收到 TaskDescription 之后，首先需要对 TaskDescription 反序列化才能读取任务信息，然后将任务代码再反序列化得到可执行代码，最后再结合其他任务信息创建TaskRunner。当数据过于分散，分布式任务数量会大幅增加，但每个任务需要处理的数据量却少之又少，就 CPU 消耗来说，相比花在数据处理上的比例，任务调度上的开销几乎与之分庭抗礼。显然，在这种情况下，CPU 的有效利用率也是极低的。

1.并行度较低

并行度较低，数据分片较大，容易导致cpu线程挂起，如果每个executor存在4个vcore，内存10g，每个task需要跑5g内存，那么实际上会有2个vcore被浪费掉。

2.并行度过高

数据过于分散会让调度开销更多。

3.如何设置比较合适？

每个并行度的数据量（总数据量/并行度） 在（Executor 内存/core 数/2, Executor 内存/core 数）区间。

如果想要让任务运行的最快当然是一个 task 对应一个 vcore,但是一般不会这样设置，为了合理利用资源，**一般会将并行度（task 数）设置成并发度（vcore 数）的 2 倍到 3 倍**。假设我们当前人物的提交参数中是12个vcode，那么将这个参数设置为24~36是比较合适的。

### 4 spark sql优化

>SparkSQL 在整个执行计划处理的过程中，使用了 Catalyst 优化器。

#### 4.1 RBO优化

> RBO：rule base Optimiztion，基于规则的优化

##### 4.1.1 谓词下推(Predicate Pushdown)

谓词指的是那些过滤条件，谓词下推就是尽可能将这些逻辑提前执行，减少下游处理的数据量，而这些规则对于Parquet，ORC这类存储格式，结合文件注脚中的统计信息，可以大幅度的减少数据扫描量，降低磁盘的I/O开销。

eg：左表left join右表

|                       | 左表       | 右表       |
| --------------------- | ---------- | ---------- |
| Join 中条件（on ）    | 只下推右表 | 只下推右表 |
| Join 后条件（where ） | 两表都下推 | 两表都下推 |

因为默认对于下推的谓词存在非空判断，因此可能会造成筛选条件放到on或者where后面的**执行结果**有所不同。

##### 4.1.2 列裁剪

扫描数据源的时候，只读取那些与查询相关的字段。

##### 4.1.3 常量替换

如果我们在select语句中参杂一些常量表达式，那么Catalyst也会自动用表达式的结果进行替换。

#### 4.2 CBO优化

##### 4.2.1 什么是CBO?

CBO 优化主要在物理计划层面，原理是计算所有可能的物理计划的代价，并挑选出代
价最小的物理执行计划，充分考虑出数据本身的特点(大小和分布)以及操作算子的代价，从而选择执行代价最小的物理执行计划。

而每个执行节点的代价，分为两个部分:

1. 该执行节点对数据集的影响，即该节点输出数据集的大小与分布
   1. 初始数据集，也即原始表，其数据集的大小与分布可直接通过统计得到。
   2. 中间节点输出数据集的大小与分布可由其输入数据集的信息与操作本身的特点推算。
2. 该执行节点操作算子的代价：代价相对固定，可以用规则来描述。

##### 4.2.2 Statistics收集

> 需要先执行特定的 SQL 语句来收集所需的表和列的统计信息。

1.表级别的统计信息(扫表)

生成 sizeInBytes 和 rowCount。

```sql
#无法计算非hdfs数据源的表的文件大小
ANALYZE TABLE 表名 COMPUTE STATISTICS
```

2.表级别的统计信息(不扫描)

只生成 sizeInBytes，如果原来已经生成过 sizeInBytes 和 rowCount，而本次生成的sizeInBytes 和原来的大小一样，则保留 rowCount（若存在），否则清除 rowCount。

```sql
ANALYZE TABLE src COMPUTE STATISTICS NOSCAN
```

3.生成列级别的信息

```sql
ANALYZE TABLE 表名 COMPUTE STATISTICS FOR COLUMNS 列 1,列 2,列 3
```

生成列统计信息，为保证一致性，会同步更新表统计信息。目前不支持复杂数据类型（如 Seq, Map 等）和 HiveStringType 的统计信息生成。

4.显示统计信息

```sql
DESC FORMATTED 表名
DESC FORMATTED 表名 列名
```

##### 4.2.3 使用CBO

通过`spark.sql.cbo.enabled`来开启，默认是 false。配置开启 CBO 后，CBO 优化器可以基于表和列的统计信息，进行一系列的估算，最终选择出最优的查询计划。比如：Build 侧择优化、优化 Join 类型、优化多表 Join 顺序等。

| 参数                                   | 描述                                                         | 默认值 |
| -------------------------------------- | ------------------------------------------------------------ | ------ |
| spark.sql.cbo.enabled                  | CBO 总开关。true 表示打开，false 表示关闭。要使用该功能，需确保相关表和列的统计信息已经生成。 | false  |
| spark.sql.cbo.joinReorder.enabled      | 使用 CBO 来自动调整连续的 inner join 的顺序。true：表示打开，false：表示关闭。要使用该功能，需确保相关表和列的统计信息已经生成，且CBO 总开关打开。 | false  |
| spark.sql.cbo.joinReorder.dp.threshold | 使用 CBO 来自动调整连续 inner join 的表的个数阈值。如果超出该阈值，则不会调整 join 顺序。 | 12     |

#### 4.3 join

##### 4.3.1 广播join

Spark join 策略中，如果当一张小表足够小并且可以先缓存到内存中，那么可以使用Broadcast Hash Join,其原理就是先将小表聚合到 driver 端，再广播到各个大表分区中，那么再次进行 join 的时候，就相当于大表的各自分区的数据与小表进行本地 join，从而规避了shuffle。

广播 join 默认值为 10MB，由 spark.sql.autoBroadcastJoinThreshold 参数控制。但是我们可以强制实现广播。

```scala
def main( args: Array[String] ): Unit = {
  val sparkConf = new SparkConf().setAppName("ForceBroadcastJoinTuning")
    .set("spark.sql.autoBroadcastJoinThreshold","-1") // 关闭自动广播
    .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
  val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)


  //TODO SQL Hint方式
  val sqlstr1 =
    """
      |select /*+  BROADCASTJOIN(sc) */
      |  sc.courseid,
      |  csc.courseid
      |from sale_course sc join course_shopping_cart csc
      |on sc.courseid=csc.courseid
    """.stripMargin

  val sqlstr2 =
    """
      |select /*+  BROADCAST(sc) */
      |  sc.courseid,
      |  csc.courseid
      |from sale_course sc join course_shopping_cart csc
      |on sc.courseid=csc.courseid
    """.stripMargin

  val sqlstr3 =
    """
      |select /*+  MAPJOIN(sc) */
      |  sc.courseid,
      |  csc.courseid
      |from sale_course sc join course_shopping_cart csc
      |on sc.courseid=csc.courseid
    """.stripMargin



  sparkSession.sql("use sparktuning;")
  println("=======================BROADCASTJOIN Hint=============================")
  sparkSession.sql(sqlstr1).explain()
  println("=======================BROADCAST Hint=============================")
  sparkSession.sql(sqlstr2).explain()
  println("=======================MAPJOIN Hint=============================")
  sparkSession.sql(sqlstr3).explain()

  // TODO API的方式
  val sc: DataFrame = sparkSession.sql("select * from sale_course").toDF()
  val csc: DataFrame = sparkSession.sql("select * from course_shopping_cart").toDF()
  println("=======================DF API=============================")
  import org.apache.spark.sql.functions._
  broadcast(sc)
    .join(csc,Seq("courseid"))
    .select("courseid")
    .explain()
}
```

##### 4.3.2 SMB join

SMB JOIN 是 sort merge bucket 操作，需要进行分桶，首先会进行排序，然后根据 key值合并，把相同 key 的数据放到同一个 bucket 中（按照 key 进行 hash）。分桶的目的其实就是把大表化成小表。相同 key 的数据都在同一个桶中之后，再进行 join 操作，那么在联合的时候就会大幅度的减小无关项的扫描。

使用要求：

- 两表进行分桶，桶的个数必须相等。
- 两边进行join的时候，join列=排序列=分桶列

```scala
def main( args: Array[String] ): Unit = {

  val sparkConf = new SparkConf().setAppName("SMBJoinTuning")
    .set("spark.sql.shuffle.partitions", "36")
  val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
  useSMBJoin(sparkSession)

}

def useSMBJoin( sparkSession: SparkSession ) = {
  //查询出三张表 并进行join 插入到最终表中
  val saleCourse = sparkSession.sql("select *from sparktuning.sale_course")
  val coursePay = sparkSession.sql("select * from sparktuning.course_pay_cluster")
    .withColumnRenamed("discount", "pay_discount")
    .withColumnRenamed("createtime", "pay_createtime")
  val courseShoppingCart = sparkSession.sql("select *from sparktuning.course_shopping_cart_cluster")
    .drop("coursename")
    .withColumnRenamed("discount", "cart_discount")
    .withColumnRenamed("createtime", "cart_createtime")

  val tmpdata = courseShoppingCart.join(coursePay, Seq("orderid"), "left")
  val result = broadcast(saleCourse).join(tmpdata, Seq("courseid"), "right")
  result
    .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
      , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
      "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "sparktuning.sale_course.dt", "sparktuning.sale_course.dn")
    .write
    .mode(SaveMode.Overwrite)
    .saveAsTable("sparktuning.salecourse_detail_2")

}
```

### 5 数据倾斜

#### 5.1 数据倾斜的现象

大多数的task的执行速度很快，但是存在几个task任务运行及其缓慢，甚至于慢慢的出现内存溢出的现象。

![image-20220105232528052](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220105232528052.png)

原因：

一般来说发生在shuffle类的算子，比如distinct，groupByKey，ReduceByKey，join等。涉及到数据的重分区，如果其中某一个key数量特别大，就发生了数据倾斜。

#### 5.2 大key定位

策略：从所有 key 中，把其中每一个 key 随机取出来一部分，然后进行一个百分比的推算，这是用局部取推算整体，虽然有点不准确，但是在整体概率上来说，我们只需要大概就可以定位那个最多的 key了。

```scala
def main( args: Array[String] ): Unit = {

  val sparkConf = new SparkConf().setAppName("BigJoinDemo")
    .set("spark.sql.shuffle.partitions", "36")
    .setMaster("local[*]")
  val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)

  println("=============================================csc courseid sample=============================================")
  val cscTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.course_shopping_cart","courseid")
  println(cscTopKey.mkString("\n"))

  println("=============================================sc courseid sample=============================================")
  val scTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.sale_course","courseid")
  println(scTopKey.mkString("\n"))

  println("=============================================cp orderid sample=============================================")
  val cpTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.course_pay","orderid")
  println(cpTopKey.mkString("\n"))

  println("=============================================csc orderid sample=============================================")
  val cscTopOrderKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.course_shopping_cart","orderid")
  println(cscTopOrderKey.mkString("\n"))
}


def sampleTopKey( sparkSession: SparkSession, tableName: String, keyColumn: String ): Array[(Int, Row)] = {
  val df: DataFrame = sparkSession.sql("select " + keyColumn + " from " + tableName)
  val top10Key = df
    .select(keyColumn).sample(false, 0.1).rdd // 对key不放回采样
    .map(k => (k, 1)).reduceByKey(_ + _) // 统计不同key出现的次数
    .map(k => (k._2, k._1)).sortByKey(false) // 统计的key进行排序
    .take(10)
  top10Key
}
```

#### 5.3 倾斜优化

##### 5.3.1 单表数据倾斜优化

为了减少shuffle数据量以及reduce端的压力，Spark sql通常是预聚合+exchange+reduce端聚合，所以执行计划中`HashAggregate`通常是成对出现的。

解决方式：两阶段聚合(加盐局部聚合，去盐全局聚合)。

##### 5.3.2 Join优化

1）广播优化

适用于小表 join 大表。小表足够小，可被加载进 Driver 并通过 Broadcast 方法广播到各个 Executor 中，可以直接规避掉此shuffle阶段，直接优化掉stage，而且广播join也是SparkSql中最常用的优化方案。

2）拆分大key打散小表

解决逻辑

1. 将存在倾斜的表，根据抽样结果，拆分为倾斜 key（skew 表）和没有倾斜 key（common）的两个数据集。
2. 将 skew 表的 key 全部加上随机前缀，然后对另外一个不存在严重数据倾斜的数据集（old 表）整体与随机前缀集作笛卡尔乘积（即将数据量扩大 N 倍，得到 new 表）。
3. 打散的 skew 表 join 扩容的 new 表 union Common 表 join old 表

实现思路：

1. 打散大表：实际就是数据一进一出进行处理，对大 key 前拼上随机前缀实现打散。
2. 扩容小表：实际就是将 DataFrame 中每一条数据，转成一个集合，并往这个集合里循环添加 10 条数据，最后使用 flatmap 压平此集合，达到扩容的效果.

### 6 Job优化

![image-20220105234710666](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220105234710666.png)

#### 6.1 Map端优化

##### 6.1.1 Map 端聚合

SparkSQL 本身的 HashAggregte 就会实现本地预聚合+全局聚合。

##### 6.1.2 读取小文件优化

读取的数据源有很多小文件，会造成查询性能的损耗，大量的数据分片信息以及对应产生的 Task 元信息也会给 Spark Driver 的内存造成压力，带来单点问题。

参数：

- spark.sql.files.maxPartitionBytes=128MB 默认 128m：文件最大分区字节数。
- spark.files.openCostInBytes=4194304 默认 4m：打开一个文件的开销。

![image-20220105235132047](https://gitee.com/code1997/blog-image/raw/master/bigdata/image-20220105235132047.png)

解析：

1. 切片大小= Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))，计算 totalBytes 的时候，每个文件都要加上一个 open 开销defaultParallelism 就是 RDD 的并行度。
2. 当（文件 1 大小+ openCostInBytes）+（文件 2 大小+ openCostInBytes）+…+（文件n-1 大小+ openCostInBytes）+ 文件 n <= maxPartitionBytes 时，n 个文件可以读入同一个分区，即满足： N 个小文件总大小 + （N-1）*openCostInBytes <= maxPartitionBytes 的话。6.2.

##### 6.1.3 增大map 溢写 时流 输出流 buffer

1）map 端 Shuffle Write 有一个缓冲区，初始阈值 5m，超过会尝试增加到 2*当前使用内存。如果申请不到内存，则进行溢写。是 这个参数是 internal ，指定） 无效（见下方源码）。也就是说资源足够会自动扩容，所以不需要我们去设置。

2）溢写时使用输出流缓冲区默认 32k，这些缓冲区减少了磁盘搜索和系统调用次数，适当提高可以提升溢写效率。

3）shuffle 文件涉及到序列化，是采取批的方式读写，默认按照每批次 1 万条去读写。设置得太低会导致在序列化时过度复制，因为一些序列化器通过增长和复制的方式来翻倍内部数据结构。这个参数是 internal，指定无效。

#### 6.2 reduce端优化

##### 6.2.1 合理设置reduce数

过多的 cpu 资源出现空转浪费，过少影响任务性能。关于并行度、并发度的相关参数介绍，参照之前的介绍。

##### 6.2.2 输出产生小文件优化

join 结果插入新表，生成的文件数等于 shuffle 并行度，默认就是 200 份文件插入到hdfs 上。
