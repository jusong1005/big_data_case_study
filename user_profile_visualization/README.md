# 用户画像可视化

该模块是广电用户画像展示服务，后端读取 `big_data_case_study.user_label` 和 `user_profile_svm_metrics`，前端提供用户画像查询和实时大屏。

## WSL 启动后端

```bash
cd /mnt/d/code/big_data_case_study/trunk/user_profile_visualization
bash run_visualization_wsl.sh
```

默认连接：

```text
MySQL: localhost:3306/big_data_case_study
User: bigdata
Password: BigData@123456
Backend: http://localhost:8001/user_profile
```

可用环境变量覆盖连接信息：

```bash
MYSQL_HOST=localhost MYSQL_DATABASE=big_data_case_study MYSQL_USERNAME=bigdata MYSQL_PASSWORD='BigData@123456' bash run_visualization_wsl.sh
```

## WSL 构建前端

```bash
cd /mnt/d/code/big_data_case_study/trunk/user_profile_visualization/frontend-vue
bash build_wsl.sh
```

`build_wsl.sh` 会把 Node.js 安装到 `/home/hadoop/export/servers`，不需要 sudo。

## API 验证

后端启动后可以验证：

```bash
curl http://localhost:8001/user_profile/api/screen/summary
curl http://localhost:8001/user_profile/api/screen/parent-stats
curl http://localhost:8001/user_profile/api/labels/samples
```

前端开发服务仍使用：

```bash
npm run dev
```

如果 Windows 没有 Node/npm，可先在 WSL 执行 `build_wsl.sh`，或把 `/home/hadoop/export/servers/node-v20.19.5-linux-x64/bin` 加入 WSL `PATH` 后再运行前端命令。
