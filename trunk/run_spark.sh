#!/usr/bin/env bash
spark-submit \
--driver-class-path /export/servers/hive-1.2.1/lib/mysql-connector-java-5.1.45.jar \
--class com.tipdm.scala.chapter_3_6_processing.DataProcess \
--master local[*] \
/export/data/big_data_case_study_project/user_profile_project-1.0.jar \
media_index_3m media_1d \
mediamatch_userevent_3m mediamatch_userevent_1d \
mediamatch_usermsg_3m mediamatch_usermsg_1d \
mmconsume_billevents_3m mmconsume_billevents_1d \
order_index_v3 order_index_1d
