import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// 示例列名，数据结构定义
val networkFields: Array[String] = Array("frame.number", "frame.protocols", "ethernet.source_mac", "ip.source_ip", "ip.destination_ip")
val dnsRecordFields: Array[String] = Array("query.transaction_id", "query.opcode", "response.id", "response.flags")

// 预处理函数
def Preprocess(df: DataFrame): DataFrame = {
  val processedDf = df.withColumn("sourceIpAddress", $"network.ip.source_ip")
    .withColumn("destinationIpAddress", $"network.ip.destination_ip")
    .withColumn("dnsRecordList", $"dns_records")
  processedDf
}

// GetDnsInfos函数
def GetDnsInfos(df: DataFrame): DataFrame = {
  var retdf = df
    .select((networkFields ++ Array("dnsRecordList") ++ dnsRecordFields).map(col(_)): _*)
    .filter(col("sourceIpAddress").isNotNull and col("destinationIpAddress").isNotNull)

  retdf = retdf.withColumn("record", explode_outer(col("dnsRecordList"))).distinct.drop("dnsRecordList")

  // 提取DNS记录的详细信息
  retdf = retdf.select($"record.query.*", $"record.response.*")
    .withColumn("A", explode_outer($"record.response.answers").as("dnsA"))
    .withColumn("AAAA", explode_outer($"record.response.answers").as("dnsAAAA"))
    .withColumn("CNAME", explode_outer($"record.response.answers").as("dnsCNAME"))
    .withColumn("MX", explode_outer($"record.response.answers").as("dnsMX"))
    .withColumn("NS", explode_outer($"record.response.answers").as("dnsNS"))
    .withColumn("TXT", explode_outer($"record.response.answers").as("dnsTXT"))
    .withColumn("SOA", explode_outer($"record.response.answers").as("dnsSOA"))
    .withColumn("SRV", explode_outer($"record.response.answers").as("dnsSRV"))
    .withColumn("PTR", explode_outer($"record.response.answers").as("dnsPTR"))
    .drop("record")

  retdf
}

// 读取JSON数据
val jsonPath = "/root/spark/data/dns_records.json" // 替换为你的JSON文件路径
val rawDf = spark.read.json(jsonPath)

// 转换为Parquet格式（在CPU模式下）
rawDf.write.parquet("/root/spark/data/dns_records.parquet")

// 使用GPU加速读取Parquet文件
val gpuDf = spark.read.parquet("/root/spark/data/dns_records.parquet")


// 应用预处理函数
val preprocessedDf = Preprocess(gpuDf)

// 应用GetDnsInfos函数处理数据
val dnsInfosDf = GetDnsInfos(preprocessedDf)

// 展示处理后的数据
dnsInfosDf.show(false)
