#!/bin/bash
#任务启动的日期
task_start_time=$1
#hive脚本
hive_script=$2
#shell脚本开始运行的时间
start_time=$(date +%Y-%m-%d-%H:%M:%S)
#日志保存的路径
log_file=/root/qwm/order_index/log.out
echo -e "\nstart time : $start_time" >> $log_file
echo "program run day: $task_start_time" >> $log_file
#任务启动的日期与数据的开始日期（2018-01-01）间隔的天数
datatime=$3
time1=$(($(date +%s -d $task_start_time) - $(date +%s -d $datatime)))
delta=$((time1/(60*60*24)))
echo "任务启动的日期与数据的开始日期（2018-01-01）间隔的天数:$delta" >> $log_file
#当前日期与任务启动的日期间隔的天数
curr_ymd=$(date +%Y-%m-%d)
curr_time2=$(($(date +%s -d $curr_ymd) - $(date +%s -d $task_start_time)))
curr_delta=$((curr_time2/(60*60*24)))
#当前日期与数据开始日期间隔的天数
curr_time3=$(($(date +%s -d $curr_ymd) - $(date +%s -d $datatime)))
curr_delta1=$((curr_time3/(60*60*24)))
echo "当前日期与任务日期的日期${task_start_time}间隔的天数:$curr_delta" >> $log_file
echo "当前日期与数据开始日期2018-01-01间隔的天数:$curr_delta1" >> $log_file
#在hive中创建表order_index_1d,比如今天(task_start_time)是2018-10-09,则order_index_1d中的数据是原始数据中optdate字段为2018-01-01这一天的数据,并且计算当前日期与optdate字段的时间差delta,然后orderdate,expdate,effdate字段分别加上时间差delta
echo -e "drop table if exists order_index_1d;\ncreate table order_index_1d as select phone_no,owner_name,concat(CURRENT_DATE,' ',from_unixtime(unix_timestamp(optdate),\"HH:mm:ss\")) as optdate,prodname,sm_name,offerid,offername,business_name,owner_code,prodprcid,prodprcname,concat(date_add(effdate,datediff(CURRENT_DATE,optdate)),' ',from_unixtime(unix_timestamp(effdate),\"HH:mm:ss\")) as effdate,concat(date_add(expdate,datediff(CURRENT_DATE,optdate)),' ',from_unixtime(unix_timestamp(expdate),\"HH:mm:ss\")) as expdate,concat(date_add(orderdate,datediff(CURRENT_DATE,optdate)),' ',from_unixtime(unix_timestamp(orderdate),\"HH:mm:ss\")) as orderdate,cost,mode_time,prodstatus,run_name,orderno,offertype from order_index_v3 where date_add(optdate,${delta}) = date_sub(CURRENT_DATE,${curr_delta});">${hive_script}
hive -f /root/qwm/order_index/${hive_script}
#将order_index_1d表的数据导入到es
spark-submit --class com.tipdm.scala.datasource.Hive2Elasticsearch --executor-memory 10g --total-executor-cores 20 --master spark://$4:7077 --jars /opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/spark/lib/elasticsearch-spark-13_2.10-6.3.2.jar $5 order_index_1d $6 $7 $8 $9 ${10}
end_time=$(date +%Y-%m-%d-%H:%M:%S)
echo "end time : $end_time" >> $log_file