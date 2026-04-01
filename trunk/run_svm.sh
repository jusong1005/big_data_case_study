#!/usr/bin/env bash
spark-submit \
--driver-class-path /export/servers/hive-1.2.1/lib/mysql-connector-java-5.1.45.jar \
--class com.tipdm.scala.chapter_3_8_6_svm.SVM \
--master local[*] \
/export/data/big_data_case_study_project/user_profile_project-1.0.jar \
mmconsume_billevents_1d \
mediamatch_userevent_1d \
media_1d \
mediamatch_usermsg_1d \
order_index_1d \
svm_activate \
svm_prediction \
100 \
1.0 \
0.01 \
1.0 \
default
