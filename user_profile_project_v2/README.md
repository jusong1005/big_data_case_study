# user_profile_project_v2

这是用户画像训练模型目录的 Spark 3 重构版，迁移旧工程 `processing/DataProcess.scala`、`chapter_3_8_6_svm` / `processing/SVMDataProcess.scala` 和 `LabelSQL.java` 的核心链路，目标是在 WSL 本地可直接构建和运行。

## 已重构内容

- 不再依赖 CDH、HiveContext、Spark 1.6、Scala 2.10。
- 使用 JDK 17、Scala 2.12.18、Spark 3.5.1、MySQL Connector/J 8.4.0。
- 读取 5 张 CSV 样本并输出清洗后的 Parquet 中间表。
- 构造 `consume`、`join_time`、`count_duration` 三个训练特征。
- 使用 Spark ML `LinearSVC` 训练用户是否挽留模型。
- 生成消费内容、消费水平、产品、品牌、入网程度和用户挽留等画像标签。
- 输出 Parquet：训练特征、预测结果、评估指标、Pipeline 模型。
- 输出 MySQL：`user_profile_svm_metrics`、`user_profile_svm_prediction`、`user_label`。

## WSL 运行

```bash
cd /mnt/d/code/big_data_case_study/trunk/user_profile_project_v2
bash bin/run_retention_model_wsl.sh
```

脚本默认使用 `/home/hadoop/export/data/user_profile_project_v2/raw/media_index_sample`。如果该目录还没有样本，会从 `/mnt/d/BaiduNetdiskDownload/media_index_test` 为每张 CSV 抽取表头和前 10000 行数据。

如需指定别的数据目录：

```bash
DATA_RAW_PATH=/path/to/media_index_sample bash bin/run_retention_model_wsl.sh
```

如需从别的原始目录重新抽样：

```bash
ORIGINAL_DATA_DIR=/path/to/media_index_test SAMPLE_LINES=20001 bash bin/run_retention_model_wsl.sh
```

验证输出：

```bash
bash bin/verify_outputs_wsl.sh
```

可视化服务读取的兼容表仍是 `user_label`，其中 `parent_label='用户是否挽留'`。

## MySQL 表

画像主线只需要保留：

```text
user_profile_svm_metrics
user_profile_svm_prediction
user_label
```

如果库里还有 `law_v2_*` 或 `spark_test`，它们是早期环境验证或法律推荐示例表，不影响画像链路。
