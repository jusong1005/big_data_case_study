# 用户画像脚本目录

当前主线已迁移到 `user_profile_project_v2`，本目录保留旧版 Logstash、Kafka、Hive 和 XXL-JOB 脚本作为历史参考。

## WSL 本地运行主线

```bash
cd /mnt/d/code/big_data_case_study/trunk/user_profile_scripts
bash run_user_profile_v2_wsl.sh
```

该入口会依次执行：

1. `user_profile_project_v2/bin/run_retention_model_wsl.sh`
2. `user_profile_project_v2/bin/verify_outputs_wsl.sh`

默认输出会写到：

```text
/home/hadoop/export/data/user_profile_project_v2/processed
/home/hadoop/export/data/user_profile_project_v2/model
big_data_case_study.user_profile_svm_metrics
big_data_case_study.user_profile_svm_prediction
big_data_case_study.user_label
```

## 旧脚本状态

- `csv2elasticsearch/`：旧版 Logstash 导入 Elasticsearch 配置。
- `elasticsearch2kafka/`：旧版 Elasticsearch 到 Kafka 配置。
- `timed_data_generation/`：旧版 Hive/XXL-JOB 定时生成脚本。

这些脚本依赖旧集群组件，不参与当前 WSL 本地主线。
