import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// 预处理函数
def Preprocess(df: DataFrame): DataFrame = {
  df.withColumn("sourceIpAddress", $"network.ip.source_ip")
    .withColumn("destinationIpAddress", $"network.ip.destination_ip")
    .withColumn("dnsRecordList", $"dns_records")
}

// GetDnsInfos函数
def GetDnsInfos(df: DataFrame): DataFrame = {
  val explodedDf = df.withColumn("record", explode_outer($"dnsRecordList"))
  
  val selectedDf = explodedDf.select(
    col("network.frame.number").as("frame_number"),
    col("network.frame.protocols").as("frame_protocols"),
    col("network.ethernet.source_mac").as("source_mac"),
    col("sourceIpAddress"),
    col("destinationIpAddress"),
    $"record.query.transaction_id",
    $"record.query.opcode",
    $"record.response.id",
    $"record.response.flags",
    explode_outer($"record.response.answers").as("dnsAnswers")
  )

  selectedDf.select(
    col("frame_number"),
    col("frame_protocols"),
    col("source_mac"),
    col("sourceIpAddress"),
    col("destinationIpAddress"),
    col("transaction_id"),
    col("opcode"),
    col("id"),
    col("flags"),
    col("dnsAnswers.type").as("dns_type"),
    col("dnsAnswers.name").as("dns_name"),
    col("dnsAnswers.ttl").as("dns_ttl")
  )
}

// 读取JSON数据并转换为Parquet格式（在CPU模式下）
val jsonPath = "/root/spark/data/dns_records.json" // 替换为你的JSON文件路径
val rawDf = spark.read.json(jsonPath)
rawDf.write.mode("overwrite").parquet("/root/spark/data/dns_records.parquet")

// 使用GPU加速读取Parquet文件
val gpuDf = spark.read.parquet("/root/spark/data/dns_records.parquet")

// 应用预处理函数
val preprocessedDf = Preprocess(gpuDf)

// 应用GetDnsInfos函数处理数据
val dnsInfosDf = GetDnsInfos(preprocessedDf)

// 展示处理后的数据
dnsInfosDf.show(false)
