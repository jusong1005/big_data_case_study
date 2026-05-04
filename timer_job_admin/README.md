# timer_job_admin

该模块是旧版 XXL-JOB 管理后台，使用 Spring MVC、Quartz、Tomcat7 插件和旧 MySQL Connector。

## 当前状态

当前 WSL 本地用户画像主线不启动该后台。离线计算和可视化已经改为直接运行：

```text
user_profile_project_v2
user_profile_visualization
```

如需运行当前主线：

```bash
cd /mnt/d/code/big_data_case_study/trunk/user_profile_scripts
bash run_user_profile_v2_wsl.sh

cd /mnt/d/code/big_data_case_study/trunk/user_profile_visualization
bash run_visualization_wsl.sh
```

## 保留原因

该模块作为旧调度平台源码参考保留，不参与当前 WSL 本地构建验收。
