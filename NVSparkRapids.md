# NV平台的Spark Rapids测试平台搭建
## 硬件平台
    cpu:Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz
    内存：48G
    显卡：一个 nv tesla T4 
## 软件平台
    os: Ubuntu 22.04.3 LTS
    nv driver: 535.129.03
    CUDA Version: 12.2
    Spark Version: 3.4.2
    Spark Rapids Version:rapids-4-spark_2.12-23.10.0
## 运行spark命令
    ${SPARK_HOME}/bin/spark-shell --jars  /root/spark/rapids-4-spark_2.12-23.10.0.jar --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.concurrentGpuTasks=2
## 运行测试脚本
    :load /root/spark/code/sparkrapids/sparkrapidsTest.scala

## 执行日志（gpu 执行部分）
    df: org.apache.spark.sql.DataFrame = [frame.number: string, frame.time_epoch: string ... 5 more fields]
root
 |-- frame.number: string (nullable = true)
 |-- frame.time_epoch: string (nullable = true)
 |-- ip.src: string (nullable = true)
 |-- tcp.srcport: string (nullable = true)
 |-- ip.dst: string (nullable = true)
 |-- tcp.dstport: string (nullable = true)
 |-- _ws.col.Protocol: string (nullable = true)

filteredDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [frame.number: string, frame.time_epoch: string ... 5 more fields]
== Physical Plan ==
GpuColumnarToRow false
+- GpuFilter ((ip.src#37 = 192.168.5.162) OR (ip.dst#39 = 192.168.5.162))
   +- GpuFileGpuScan csv [frame.number#35,frame.time_epoch#36,ip.src#37,tcp.srcport#38,ip.dst#39,tcp.dstport#40,_ws.col.Protocol#41] Batched: true, DataFilters: [((ip.src#37 = 192.168.5.162) OR (ip.dst#39 = 192.168.5.162))], Format: CSV, Location: InMemoryFileIndex[file:/root/spark/data/network_data.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<frame.number:string,frame.time_epoch:string,ip.src:string,tcp.srcport:string,ip.dst:string...


df1Renamed: org.apache.spark.sql.DataFrame = [frame_number1: string, frame.time_epoch: string ... 5 more fields]
df2Renamed: org.apache.spark.sql.DataFrame = [frame_number2: string, frame.time_epoch: string ... 5 more fields]
joinedDF: org.apache.spark.sql.DataFrame = [frame_number1: string, frame.time_epoch: string ... 12 more fields]
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- SortMergeJoin [ip.src#37, ip.dst#39], [ip.src#84, ip.dst#86], Inner, (NOT (protocol1#58 = protocol2#74) AND (frame_number1#50 < frame_number2#66))
   :- Sort [ip.src#37 ASC NULLS FIRST, ip.dst#39 ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(ip.src#37, ip.dst#39, 200), ENSURE_REQUIREMENTS, [plan_id=97]
   :     +- Project [frame.number#35 AS frame_number1#50, frame.time_epoch#36, ip.src#37, tcp.srcport#38, ip.dst#39, tcp.dstport#40, _ws.col.Protocol#41 AS protocol1#58]
   :        +- Filter ((((((ip.src#37 = 192.168.5.162) OR (ip.dst#39 = 192.168.5.162)) AND isnotnull(ip.src#37)) AND isnotnull(ip.dst#39)) AND isnotnull(_ws.col.Protocol#41)) AND isnotnull(frame.number#35))
   :           +- FileScan csv [frame.number#35,frame.time_epoch#36,ip.src#37,tcp.srcport#38,ip.dst#39,tcp.dstport#40,_ws.col.Protocol#41] Batched: false, DataFilters: [((ip.src#37 = 192.168.5.162) OR (ip.dst#39 = 192.168.5.162)), isnotnull(ip.src#37), isnotnull(ip..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/root/spark/data/network_data.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<frame.number:string,frame.time_epoch:string,ip.src:string,tcp.srcport:string,ip.dst:string...
   +- Sort [ip.src#84 ASC NULLS FIRST, ip.dst#86 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(ip.src#84, ip.dst#86, 200), ENSURE_REQUIREMENTS, [plan_id=98]
         +- Project [frame.number#82 AS frame_number2#66, frame.time_epoch#83, ip.src#84, tcp.srcport#85, ip.dst#86, tcp.dstport#87, _ws.col.Protocol#88 AS protocol2#74]
            +- Filter ((((((ip.src#84 = 192.168.5.162) OR (ip.dst#86 = 192.168.5.162)) AND isnotnull(ip.src#84)) AND isnotnull(ip.dst#86)) AND isnotnull(_ws.col.Protocol#88)) AND isnotnull(frame.number#82))
               +- FileScan csv [frame.number#82,frame.time_epoch#83,ip.src#84,tcp.srcport#85,ip.dst#86,tcp.dstport#87,_ws.col.Protocol#88] Batched: false, DataFilters: [((ip.src#84 = 192.168.5.162) OR (ip.dst#86 = 192.168.5.162)), isnotnull(ip.src#84), isnotnull(ip..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/root/spark/data/network_data.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<frame.number:string,frame.time_epoch:string,ip.src:string,tcp.srcport:string,ip.dst:string...




## 1亿条模拟网络抓包记录脚本
    python3 data.py
    


