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
    
