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
      $"network.frame.number".as("number"),
      $"network.frame.protocols".as("protocols"),
      $"network.ethernet.source_mac".as("source_mac"),
      $"network.ethernet.destination_mac".as("destination_mac"),
      $"network.ip.source_ip".as("source_ip"),
      $"network.ip.destination_ip".as("destination_ip"),
      $"network.udp.source_port".as("source_port"),
      $"network.udp.destination_port".as("destination_port"),
      explode($"dnsRecordList.query.queries").as("query"),
      $"dnsRecordList.response.*"
    )

  // 从展开的记录中提取信息
  val retdf = expandedDf
    .withColumn("id", $"query.transaction_id")
    .withColumn("name", $"query.name")
    .withColumn("ttl", $"query.ttl")
    .withColumn("type", $"query.type")
    .withColumn("QR", $"response.QR")
    .withColumn("rCode", $"response.reply_code")
    .withColumn("section", $"response.section")
    .withColumn("dnsA", $"response.dnsA")
    .withColumn("dnsAAAA", $"response.dnsAAAA")
    .withColumn("dnsCNAME", $"response.dnsCNAME")
    .withColumn("dnsMX", $"response.dnsMX")
    .withColumn("dnsNS", $"response.dnsNS")
    .withColumn("dnsTXT", $"response.dnsTXT")
    .withColumn("dnsSOA", $"response.dnsSOA")
    .withColumn("dnsSRV", $"response.dnsSRV")
    .withColumn("dnsPTR", $"response.dnsPTR")

  // 选择相关列
  retdf.select((networkFields ++ Array("id", "name", "ttl", "QR", "type", "rCode", "section", "dnsA", "dnsAAAA", "dnsCNAME", "dnsMX", "dnsNS", "dnsTXT", "dnsSOA", "dnsSRV", "dnsPTR")).map(col): _*)
}

// 应用GetDnsInfos函数处理数据
val dnsInfosDf = GetDnsInfos(preprocessedDf)

// 展示处理后的数据
dnsInfosDf.show(false)
