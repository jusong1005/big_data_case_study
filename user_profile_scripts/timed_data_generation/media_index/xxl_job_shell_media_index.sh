#!/bin/bash
#设置任务启动的日期
task_start_time=$1
#hive脚本
hive_script=$2
#shell脚本开始运行的时间
start_time=$(date +%Y-%m-%d-%H:%M:%S)
log_file=/root/qwm/log.out
echo -e "\nstart time : $start_time" >> $log_file
echo "program run day: $task_start_time" >> $log_file
#任务启动的日期与数据的开始日期（2018-05-02）间隔的天数
datatime=$3
time1=$(($(date +%s -d $task_start_time) - $(date +%s -d $datatime)))
delta=$((time1/(60*60*24)))
echo "任务启动的日期与数据的开始日期（2018-05-02）间隔的天数:$delta" >> $log_file
#当前日期与任务启动的日期间隔的天数
curr_ymd=$(date +%Y-%m-%d)
curr_time2=$(($(date +%s -d $curr_ymd) - $(date +%s -d $task_start_time)))
curr_delta=$((curr_time2/(60*60*24)))
echo "当前日期与任务启动的日期${task_start_time}间隔的天数:$curr_delta" >> $log_file
#在hive中创建表media_1d,比如今天(task_start_time)是2018-09-18,则media_1d中的数据是选择原始数据中2018-05-02这一天的数据，并且其中origin_time和end_time的日期都改为2018-09-18,到了明天2018-09-19,则media_1d中的数据是2018-05-03这一天的数据,并且其中origin_time和end_time的日期都改为2018-09-19
echo -e "drop table if exists media_1d;\ncreate table media_1d as select terminal_no,phone_no,duration,station_name,concat(CURRENT_DATE,' ',from_unixtime(unix_timestamp(origin_time),\"HH:mm:ss\")) as origin_time,concat(CURRENT_DATE,' ',from_unixtime(unix_timestamp(end_time),\"HH:mm:ss\")) as end_time,owner_code,owner_name,vod_cat_tags,resolution,audio_lang,region,res_name,res_type,vod_title,category_name,program_title,sm_name,first_show_time from media_index_3m where date_add(origin_time,${delta}) = date_sub(CURRENT_DATE,${curr_delta});">${hive_script}
hive -f /root/qwm/${hive_script}
#将media_1d表的数据导入到es
spark-submit --class com.tipdm.scala.datasource.Hive2Elasticsearch --executor-memory 10g --total-executor-cores 20 --master spark://$4:7077 --jars /opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/spark/lib/elasticsearch-spark-13_2.10-6.3.2.jar $5 media_1d $6 $7 $8 $9 ${10}
#shell脚本结束运行的时间
end_time=$(date +%Y-%m-%d-%H:%M:%S)
echo "end time : $end_time" >> $log_file
