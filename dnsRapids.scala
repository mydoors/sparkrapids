import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// 定义网络字段
val networkFields: Array[String] = Array(
  "network.frame.number", "network.frame.protocols",
  "network.ethernet.source_mac", "network.ethernet.destination_mac",
  "network.ip.source_ip", "network.ip.destination_ip",
  "network.udp.source_port", "network.udp.destination_port"
)

// 预处理函数
def Preprocess(df: DataFrame): DataFrame = {
  df.withColumn("silkAppLabel", lit(53))
    .withColumn("sourceIpAddress", $"network.ip.source_ip")
    .withColumn("destinationIpAddress", $"network.ip.destination_ip")
    .withColumn("dnsRecordList", explode($"dns_records"))

}

// GetDnsInfos函数
def GetDnsInfos(df: DataFrame): DataFrame = {
    var filteredDf = df.filter($"silkAppLabel" === 53 && $"sourceIpAddress".startsWith("192.168.1"))
    .select((networkFields ++ Array("dnsRecordList")).map(col): _*)
    

  // 展开dnsRecords并选择query和response的子字段
    val expandedDf = filteredDf
    .select(
      col("network.frame.number").as("number"),
      col("network.frame.protocols").as("protocols"),
      col("network.ethernet.source_mac").as("source_mac"),
      col("network.ethernet.destination_mac").as("destination_mac"),
      col("network.ip.source_ip").as("source_ip"),
      col("network.ip.destination_ip").as("destination_ip"),
      col("network.udp.source_port").as("source_port"),
      col("network.udp.destination_port").as("destination_port"),
      col("dnsRecordsExploded.query.*"),
      col("dnsRecordsExploded.response.*")
    )

  // 从展开的记录中提取信息
  var retdf = expandedDf
    .withColumn("id", $"transaction_id")
    .withColumn("name", explode_outer($"queries.name"))
    .withColumn("ttl", explode_outer($"answers.ttl"))
    .withColumn("QR", $"response")
    .withColumn("type", explode_outer($"queries.type"))
    .withColumn("rCode", $"reply_code")
    .withColumn("section", $"section")
    .withColumn("dnsA", $"dnsA")
    .withColumn("dnsAAAA", $"dnsAAAA")
    .withColumn("dnsCNAME", $"dnsCNAME")
    .withColumn("dnsMX", $"dnsMX")
    .withColumn("dnsNS", $"dnsNS")
    .withColumn("dnsTXT", $"dnsTXT")
    .withColumn("dnsSOA", $"dnsSOA")
    .withColumn("dnsSRV", $"dnsSRV")
    .withColumn("dnsPTR", $"dnsPTR")
    .drop("query", "response")

  // 处理SOA, SRV, PTR等
  retdf = retdf
    .withColumn("A", explode_outer($"dnsA")).drop("dnsA").distinct
    .withColumn("AAAA", explode_outer($"dnsAAAA")).drop("dnsAAAA").distinct
    .withColumn("CNAME", explode_outer($"dnsCNAME")).drop("dnsCNAME").distinct
    .withColumn("MX", explode_outer($"dnsMX")).drop("dnsMX").distinct
    .withColumn("NS", explode_outer($"dnsNS")).drop("dnsNS").distinct
    .withColumn("TXT", explode_outer($"dnsTXT")).drop("dnsTXT").distinct
    .withColumn("SOA", explode_outer($"dnsSOA")).drop("dnsSOA").distinct
    .withColumn("SRV", explode_outer($"dnsSRV")).drop("dnsSRV").distinct
    .withColumn("PTR", explode_outer($"dnsPTR")).drop("dnsPTR").distinct

  // 选择相关列
  retdf = retdf.select((networkFields ++ Array("id", "name", "ttl", "QR", "type", "rCode", "section", "A", "AAAA", "CNAME", "MX", "NS", "TXT", "SOA", "SRV", "PTR")).map(col): _*)
  retdf
}


// 读取JSON数据并转换为DataFrame
val jsonPath = "/root/spark/data/dns_records.json"
val rawDf = spark.read.json(jsonPath)

// 将DataFrame转换为Parquet格式
//rawDf.write.mode("overwrite").parquet("/root/spark/data/dns_records.parquet")


// 使用GPU加速读取Parquet文件
// val gpuDf = spark.read.parquet("/root/spark/data/dns_records.parquet")

// 应用预处理函数
val preprocessedDf = Preprocess(rawDf)

// 应用GetDnsInfos函数处理数据
val dnsInfosDf = GetDnsInfos(preprocessedDf)

// 展示处理后的数据
dnsInfosDf.show(false)

