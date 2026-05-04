# timer_job_java_executor

该模块是旧版 XXL-JOB Java 执行器，内部仍调用 `spark_hadoop_common` 的 CDH/YARN 提交器，并包含旧 Hive、Elasticsearch、Kafka、PostgreSQL/MySQL 配置。

## 当前状态

当前 WSL 本地用户画像主线不再依赖本模块。用户画像离线 ETL、SVM 模型和标签生成已迁移到：

```text
user_profile_project_v2
```

主线入口：

```bash
cd /mnt/d/code/big_data_case_study/trunk/user_profile_scripts
bash run_user_profile_v2_wsl.sh
```

## 保留原因

- 保留旧 XXL-JOB 调度代码作为原项目参考。
- 不在 WSL 本地链路中启动旧 CDH/YARN/Hive 任务。
- 不建议继续使用本模块里的硬编码账号、旧 HDFS 路径和旧 Spark 1.x 作业参数。
