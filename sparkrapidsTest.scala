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
// Map 操作：Filter
val filteredDF = df.filter($"`ip.src`" === "192.168.5.162")

//对同一个 DataFrame 执行自连接（self-join）以查找具有相同源 IP 和目标 IP 但协议不同的记录

val df1Renamed = filteredDF.withColumnRenamed("_ws.col.Protocol", "protocol1")
val df2Renamed = filteredDF.withColumnRenamed("_ws.col.Protocol", "protocol2")
val joinedDF = df1Renamed.alias("df1").join(df2Renamed.alias("df2"),
  $"df1.`ip.src`" === $"df2.`p.src`" &&
  $"df1.`ip.dst`" === $"df2.`ip.dst`" &&
  $"df1.protocol1" =!= $"df2.protocol2"
)


joinedDF.show()

val resultDF = joinedDF.select(
  col("`ip.src`"), 
  col("`tcp.srcport`"), 
  col("`ip.dst`"), 
  col("`tcp.dstport`"), 
  col("`protocol1`")  // 使用重命名后的列名
)


resultDF.show()
//输出结果到文件
resultDF.write.format("parquet").save("/root/spark/data/results.parquet")