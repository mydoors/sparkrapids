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
  var retdf = df.filter($"silkAppLabel" === 53 && $"sourceIpAddress".startsWith("192.168.1"))
    .select((networkFields ++ Array("dnsRecordList")).map(col): _*)
    .select($"*", $"dnsRecordList.query.*", $"dnsRecordList.response.*")
    .drop("dnsRecordList")

  // 继续处理record列中的DNS记录
  retdf = retdf
    .withColumn("id", $"transaction_id")
    .withColumn("name", $"query.name")
    .withColumn("ttl", $"response.ttl")
    .withColumn("QR", $"query.response")
    .withColumn("type", $"query.type")
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
rawDf.write.parquet("/root/spark/data/dns_records.parquet")

// 使用GPU加速读取Parquet文件
val gpuDf = spark.read.parquet("/root/spark/data/dns_records.parquet")

// 应用预处理函数
val preprocessedDf = Preprocess(gpuDf)

// 应用GetDnsInfos函数处理数据
val dnsInfosDf = GetDnsInfos(preprocessedDf)

// 展示处理后的数据
dnsInfosDf.show(false)
