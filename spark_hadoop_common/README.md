# spark_hadoop_common

该模块是旧版 Spark 1.x / Hadoop 2.x / CDH 5.7 的 YARN 提交公共库，包含 `SparkYarnJob`、CDH 平台配置和旧 Hive/HDFS/YARN 资源路径。

## 当前状态

当前 WSL 本地用户画像主线不再依赖本模块。新版链路使用：

```text
user_profile_project_v2: Spark 3.5 + Scala 2.12 + Parquet + MySQL
user_profile_visualization: Spring Boot 3 + Vue 3
```

## 保留原因

- 保留原项目 CDH/YARN 提交实现作为历史参考。
- 不在本地 WSL 主线中使用旧 `SparkYarnJob`。
- 旧 `platform/cdh_*` 配置仍指向历史集群地址和旧 Spark/Hadoop jar，不适合当前 WSL 环境。
