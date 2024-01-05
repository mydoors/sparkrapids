import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

// 定义相关列名数组
val networkFields: Array[String] = Array(
  "network.frame.number", "network.frame.protocols",
  "network.ethernet.source_mac", "network.ethernet.destination_mac",
  "network.ip.source_ip", "network.ip.destination_ip",
  "network.udp.source_port", "network.udp.destination_port"
)

// 定义DNS记录相关列名数组
val dnsInfos: Array[String] = Array(
  "query.transaction_id", "query.opcode", "response.id", "response.flags"
)

// 预处理函数：用于添加label和IP地址列
def Preprocess(df: DataFrame): DataFrame = {
  df.withColumn("silkAppLabel", lit(53))
    .withColumn("sourceIpAddress", $"network.ip.source_ip")
    .withColumn("destinationIpAddress", $"network.ip.destination_ip")
    .withColumn("dnsRecordList", $"dns_records")
}

// GetDnsInfos函数：处理包含DNS记录信息的DataFrame
def GetDnsInfos(df: DataFrame): DataFrame = {
  var retdf = df.filter($"silkAppLabel" === 53)
    .select((networkFields ++ Array("dnsRecordList") ++ dnsInfos).map(col): _*)
    .filter($"sourceIpAddress".isNotNull and $"destinationIpAddress".isNotNull)

  retdf = retdf.withColumn("record", explode_outer($"dnsRecordList")).drop("dnsRecordList")

  // 提取DNS记录的详细信息
  retdf = retdf.select($"record.*")
    .withColumns(Map(
      "id" -> col("record.dnsId"),
      "name" -> col("record.dnsName"),
      "ttl" -> col("record.dnsTTL"),
      "QR" -> col("record.dnsQueryResponse"),
      "type" -> col("record.dnsRRType"),
      "rCode" -> col("record.dnsResponseCode"),
      "section" -> col("record.dnsSection"),
      "dnsA" -> col("record.dnsA"),
      "dnsAAAA" -> col("record.dnsAAAA"),
      "dnsCNAME" -> col("record.dnsCNAME"),
      "dnsMX" -> col("record.dnsMX"),
      "dnsNS" -> col("record.dnsNS"),
      "dnsTXT" -> col("record.dnsTXT"),
      "dnsSOA" -> col("record.dnsSOA"),
      "dnsSRV" -> col("record.dnsSRV"),
      "dnsPTR" -> col("record.dnsPTR")
    )).drop("record")

    // 处理SOA, SRV, PTR等
    .withColumn("A", explode_outer(col("dnsA"))).drop("dnsA").distinct
    .withColumn("AAAA", explode_outer(col("dnsAAAA"))).drop("dnsAAAA").distinct
    .withColumn("CNAME", explode_outer(col("dnsCNAME"))).drop("dnsCNAME").distinct
    .withColumn("MX", explode_outer(col("dnsMX"))).drop("dnsMX").distinct
    .withColumn("NS", explode_outer(col("dnsNS"))).drop("dnsNS").distinct
    .withColumn("TXT", explode_outer(col("dnsTXT"))).drop("dnsTXT").distinct
    .withColumn("SOA", explode_outer(col("dnsSOA"))).drop("dnsSOA").distinct
    .withColumn("SRV", explode_outer(col("dnsSRV"))).drop("dnsSRV").distinct
    .withColumn("PTR", explode_outer(col("dnsPTR"))).drop("dnsPTR").distinct

  // 选择相关列
  retdf = retdf.select((networkFields ++ dnsInfos).map(col): _*)
  retdf
}

// 读取JSON数据
val jsonPath = "/root/spark/data/dns_records.json" // 替换为你的JSON文件路径
val rawDf = spark.read.json(jsonPath)

// 转换为Parquet格式（在CPU模式下）
df.write.parquet("/root/spark/data/dns_records.parquet")

// 使用GPU加速读取Parquet文件
val gpuDf = spark.read.parquet("/root/spark/data/dns_records.parquet")

// 应用预处理函数
val preprocessedDf = Preprocess(gpuDf)

// 应用GetDnsInfos函数处理数据
val dnsInfosDf = GetDnsInfos(preprocessedDf)

// 展示处理后的数据
dnsInfosDf.show(false)
