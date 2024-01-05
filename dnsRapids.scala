import org.apache.spark.sql.functions._

// 预处理函数
def Preprocess(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
  // 添加silkAppLabel列，这里假设所有记录的label为53
  val processedDf = df.withColumn("silkAppLabel", lit(53))
    .withColumn("sourceIpAddress", $"network.ip.source_ip")
    .withColumn("destinationIpAddress", $"network.ip.destination_ip")
    .withColumn("dnsRecordList", $"dns_records")

  processedDf
}
def GetDnsInfos(): DataFrame = {
  var retdf = self.filter(col("silkAppLabel") === 53)
    .select((fiveTuples ++ Array("dnsRecordList") ++ statInfos).map(col(_)): _*)
    .filter(col("sourceIpAddress").isNotNull and col("destinationIpAddress").isNotNull)

  retdf = retdf.withColumn("record", explode_outer(col("dnsRecordList"))).distinct.drop(col("dnsRecordList"))

  retdf = retdf.withColumns(Map(
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

  // Handle SOA, SRV, PTR
  retdf = retdf.withColumn("A", explode_outer(col("dnsA"))).drop(col("dnsA")).distinct
    .withColumn("AAAA", explode_outer(col("dnsAAAA"))).drop(col("dnsAAAA")).distinct
    .withColumn("CNAME", explode_outer(col("dnsCNAME"))).drop(col("dnsCNAME")).distinct
    .withColumn("MX", explode_outer(col("dnsMX"))).drop(col("dnsMX")).distinct
    .withColumn("NS", explode_outer(col("dnsNS"))).drop(col("dnsNS")).distinct
    .withColumn("TXT", explode_outer(col("dnsTXT"))).drop(col("dnsTXT")).distinct
    .withColumn("SOA", explode_outer(col("dnsSOA"))).drop(col("dnsSOA")).distinct
    .withColumn("SRV", explode_outer(col("dnsSRV"))).drop(col("dnsSRV")).distinct
    .withColumn("PTR", explode_outer(col("dnsPTR"))).drop(col("dnsPTR")).distinct

  retdf = retdf.select((fiveTuples ++ dnsInfos ++ statInfos).map(col(_)): _*)
}
// 读取JSON数据
val jsonPath = "path_to_your_json_file.json" // 替换为你的JSON文件路径
val rawDf = spark.read.json(jsonPath)

// 应用预处理函数
val preprocessedDf = Preprocess(rawDf)

// 应用GetDnsInfos函数处理数据
val dnsInfosDf = GetDnsInfos(preprocessedDf)

// 展示处理后的数据
dnsInfosDf.show(false)