import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
// 定义网络字段
val networkFields: Array[String] = Array(
  "network.frame.number", "network.frame.protocols",
  "network.ethernet.source_mac", "network.ethernet.destination_mac",
  "network.ip.source_ip", "network.ip.destination_ip",
  "network.udp.source_port", "network.udp.destination_port"
)

// 读取JSON数据并转换为DataFrame
val jsonPath = "/root/spark/data/dns_records.json"
val rawDf = spark.read.json(jsonPath)

// 预处理函数
def Preprocess(df: DataFrame): DataFrame = {
  df.withColumn("silkAppLabel", lit(53))
    .withColumn("sourceIpAddress", $"network.ip.source_ip")
    .withColumn("destinationIpAddress", $"network.ip.destination_ip")
    .withColumn("dnsRecordList", explode($"dns_records"))
}

// 应用预处理函数
val preprocessedDf = Preprocess(rawDf)

// GetDnsInfos函数
def GetDnsInfos(df: DataFrame): DataFrame = {
  val filteredDf = df.filter($"silkAppLabel" === 53 && $"sourceIpAddress".startsWith("192.168.1"))
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
      explode($"dnsRecordList.query.queries").as("query"),
      $"dnsRecordList.response.*"
    )

  // 处理查询和响应数据
  val retdf = expandedDf
    .withColumn("id", $"transaction_id")
    .withColumn("name", $"query.name")
    .withColumn("ttl", $"query.ttl")
    .withColumn("type", $"query.type")
    .withColumn("QR", $"response")
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

  // 选择相关列
  retdf.select((networkFields ++ Array("id", "name", "ttl", "QR", "type", "rCode", "section", "dnsA", "dnsAAAA", "dnsCNAME", "dnsMX", "dnsNS", "dnsTXT", "dnsSOA", "dnsSRV", "dnsPTR")).map(col): _*)
}

// 应用GetDnsInfos函数处理数据
val dnsInfosDf = GetDnsInfos(preprocessedDf)

// 展示处理后的数据
dnsInfosDf.show(false)
