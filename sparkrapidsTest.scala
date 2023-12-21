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


val df = spark.read.format("csv").option("header", "true").load("/root/spark/data/packets.csv")
df.printSchema()
df.show()
df.count()
// Map 操作：Filter
// 过滤出 ip.src 或 ip.dst 等于 "192.168.5.16" 的记录
// Map 操作：Filter
// 过滤出 ip.src 或 ip.dst 等于 "192.168.5.16" 的记录
val filteredDF = df.filter($"`ip.src`" === "192.168.5.16" || $"`ip.dst`" === "192.168.5.16")

// 为了在自连接中区分两个 DataFrame，给 'frame.number' 字段加上别名
val df1Renamed = filteredDF.withColumnRenamed("frame.number", "frame_number1").withColumnRenamed("_ws.col.Protocol", "protocol1")
val df2Renamed = filteredDF.withColumnRenamed("frame.number", "frame_number2").withColumnRenamed("_ws.col.Protocol", "protocol2")

// 对同一个 DataFrame 执行自连接（self-join）以查找具有相同源 IP 和目标 IP 但协议不同的记录
val joinedDF = df1Renamed.alias("df1").join(df2Renamed.alias("df2"),
  $"df1.`ip.src`" === $"df2.`ip.src`" &&
  $"df1.`ip.dst`" === $"df2.`ip.dst`" &&
  $"df1.protocol1" =!= $"df2.protocol2" &&
  $"df1.frame_number1" =!= $"df2.frame_number2"  // 确保连接的是不同的帧
)

// 显示结果
joinedDF.select(
  $"df1.frame_number1",
  $"df1.`ip.src`", 
  $"df1.`tcp.srcport`", 
  $"df1.`ip.dst`", 
  $"df1.`tcp.dstport`", 
  $"df1.protocol1", 
  $"df2.protocol2"
).show()


joinedDF.count()
