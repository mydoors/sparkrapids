import org.apache.spark.sql.functions._

// 定义一个函数来进行一些 DataFrame 操作，主要是测试spark-rapids是否正常
def processWithRapids(df: org.apache.spark.sql.DataFrame): Unit = {
 
  val transformedDF = df.withColumn("uppercased", upper(col("value")))

 
  transformedDF.show()
}

// 创建一个简单的 DataFrame
val data = Seq("hello", "world", "this", "is", "rapids").toDF("value")

// 调用函数处理 DataFrame
processWithRapids(data)

// 读入数据集
val df = spark.read.format("csv").option("header", "true").load("/root/spark/data/backup/network_data.csv")
df.printSchema()

// Map 操作：Filter
// 过滤出 ip.src 或 ip.dst 等于 "192.168.5.162" 的记录
val filteredDF = df.filter($"`ip.src`" === "192.168.5.162" || $"`ip.dst`" === "192.168.5.162")
// 查看执行情况
filteredDF.explain()

// 对同一个 DataFrame 执行自连接（self-join）以查找具有相同源 IP 和目标 IP 但协议不同的记录
val df1Renamed = filteredDF.withColumnRenamed("frame.number", "frame_number1").withColumnRenamed("_ws.col.Protocol", "protocol1")
val df2Renamed = filteredDF.withColumnRenamed("frame.number", "frame_number2").withColumnRenamed("_ws.col.Protocol", "protocol2")

val joinedDF = df1Renamed.alias("df1").join(df2Renamed.alias("df2"),
  $"df1.`ip.src`" === $"df2.`ip.src`" &&
  $"df1.`ip.dst`" === $"df2.`ip.dst`" &&
  $"df1.protocol1" =!= $"df2.protocol2" &&
  $"df1.frame_number1" < $"df2.frame_number2" // 确保不是相同的记录，并且 df1 的帧号小于 df2
)
joinedDF.explain()

// 显示结果，并去除重复行
val resultsDF = joinedDF.select(
  $"df1.frame_number1",
  $"df1.`ip.src`", 
  $"df1.`tcp.srcport`", 
  $"df1.`ip.dst`", 
  $"df1.`tcp.dstport`", 
  $"df1.protocol1", 
  $"df2.protocol2"
).distinct()
resultsDF.show()

  //  resultsDF.coalesce(1)
  // .write
  // .mode("overwrite") // 设置写入模式为 "overwrite"
  // .option("header", "true") // 包含头部
  // .option("sep", ",")       // 指定分隔符，默认是逗号
  // .csv("/root/spark/data/results") // 指定输出目录



//joinedDF.count()
