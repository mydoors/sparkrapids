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

    filteredDf
}



// 应用GetDnsInfos函数处理数据
val dnsInfosDf = GetDnsInfos(preprocessedDf)

// 展示处理后的数据
dnsInfosDf.show(false)
