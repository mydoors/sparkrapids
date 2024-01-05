import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// 五元组列和其他统计信息列
val fiveTuples: Array[String] = Array("network.frame.number", "network.frame.protocols", 
                                      "network.ethernet.source_mac", "network.ethernet.destination_mac", 
                                      "network.ip.source_ip", "network.ip.destination_ip",
                                      "network.udp.source_port", "network.udp.destination_port")

// DNS信息列
val dnsInfos: Array[String] = Array("dnsId", "dnsName", "dnsTTL", "dnsQueryResponse", 
                                    "dnsRRType", "dnsResponseCode", "dnsSection", 
                                    "dnsA", "dnsAAAA", "dnsCNAME", "dnsMX", 
                                    "dnsNS", "dnsTXT", "dnsSOA", "dnsSRV", "dnsPTR")

// 预处理函数
def Preprocess(df: DataFrame): DataFrame = {
  df.withColumn("silkAppLabel", lit(53))
    .withColumn("sourceIpAddress", $"network.ip.source_ip")
    .withColumn("destinationIpAddress", $"network.ip.destination_ip")
    .withColumn("dnsRecordList", $"dns_records")
}

// GetDnsInfos函数
def GetDnsInfos(df: DataFrame): DataFrame = {
  var retdf = df.filter($"silkAppLabel" === 53 && $"sourceIpAddress".startsWith("192.168.1"))
    .withColumn("record", explode_outer($"dnsRecordList")).drop("dnsRecordList")

  // 处理DNS记录的详细信息
  retdf = retdf.select($"record.query.*", $"record.response.*")
    .withColumn("dnsId", $"query.transaction_id")
    .withColumn("dnsName", explode_outer($"query.queries.name"))
    .withColumn("dnsTTL", explode_outer($"query.queries.ttl"))
    .withColumn("dnsQueryResponse", $"query.response")
    .withColumn("dnsRRType", explode_outer($"query.queries.type"))
    .withColumn("dnsResponseCode", $"response.reply_code")
    .withColumn("dnsSection", lit("query")) // 示例列
    .withColumn("dnsA", explode_outer($"response.answers").getField("name")) // 示例处理
    // 继续处理其他dns信息
    .drop("query", "response")

  retdf = retdf.select((fiveTuples ++ dnsInfos).map(col): _*)
  retdf
}

// 读取JSON数据，转换为Parquet格式，再使用GPU读取
val jsonPath = "/root/spark/data/dns_records.json"
val rawDf = spark.read.json(jsonPath)
rawDf.write.parquet("/root/spark/data/dns_records.parquet")
val gpuDf = spark.read.parquet("/root/spark/data/dns_records.parquet")

// 应用预处理函数和GetDnsInfos函数
val preprocessedDf = Preprocess(gpuDf)
val dnsInfosDf = GetDnsInfos(preprocessedDf)
dnsInfosDf.show(false)
