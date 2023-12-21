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
val filteredDF = df.filter($"ip.src" === "192.168.5.16" || $"ip.dst" === "192.168.5.16")

// 对同一个 DataFrame 执行自连接（self-join）以查找具有相同源 IP 和目标 IP 但协议不同的记录
val df1Renamed = filteredDF.withColumnRenamed("_ws.col.Protocol", "protocol1")
val df2Renamed = filteredDF.withColumnRenamed("_ws.col.Protocol", "protocol2")

val joinedDF = df1Renamed.alias("df1").join(df2Renamed.alias("df2"),
  col("df1.`ip.src`") === col("df2.`ip.src`") &&
  col("df1.`ip.dst`") === col("df2.`ip.dst`") &&
  col("df1.protocol1") =!= col("df2.protocol2")
)



joinedDF.select(
  col("df1.`ip.src`"), 
  col("df1.`tcp.srcport`"), 
  col("df1.`ip.dst`"), 
  col("df1.`tcp.dstport`"), 
  col("df1.protocol1"), 
  col("df2.protocol2")
).show()

joinedDF.count()
