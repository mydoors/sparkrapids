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
val filteredDF = df.filter($"ip.src" === "192.168.5.162")

//对同一个 DataFrame 执行自连接（self-join）以查找具有相同源 IP 和目标 IP 但协议不同的记录
val joinedDF = filteredDf.alias("df1").join(filteredDF.alias("df2"), 
  col("df1.ip.src") === col("df2.ip.src") && 
  col("df1.ip.dst") === col("df2.ip.dst") && 
  col("df1._ws.col.Protocol") =!= col("df2._ws.col.Protocol"))


// 输出结果到文件
/ 输出结果到 CSV 文件
joinedDF.write
  .option("header", "true") // 包含头部信息
  .csv("/root/spark/data/results.csv")