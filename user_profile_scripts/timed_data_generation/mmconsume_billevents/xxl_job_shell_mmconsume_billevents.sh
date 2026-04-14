#!/bin/bash
#shell脚本开始运行的时间
start_time=$(date +%Y-%m-%d-%H:%M:%S)
log_file=/root/qwm/mmconsume/log.out
#hive脚本
hive_script=create_mmconsume_billevents_1d.hql
echo -e "shell脚本开始运行时间 : $start_time" >> $log_file
#计算当前日期与2018-07-01相差的月数
datatime=$1
curr_ymd=$(date +%Y-%m-%d)
curr_time2=$(($(date +%s -d $curr_ymd) - $(date +%s -d $datatime)))
curr_delta=$((curr_time2/(60*60*24*30)))
echo "当前日期与2018-07-01相差的月数：$curr_delta" >> $log_file
#在hive中创建表mmconsume_billevents_1d
echo -e "drop table if exists mmconsume_billevents_1d;\ncreate table mmconsume_billevents_1d as select terminal_no,phone_no,fee_code,concat(add_months(year_month,${curr_delta}),' ','00:00:00')as year_month,owner_name,owner_code,sm_name,should_pay,favour_fee from mmconsume_billevents;">${hive_script}
hive -f /root/qwm/mmconsume/create_mmconsume_billevents_1d.hql
#将mmconsume_billevents_1d表的数据以csv格式导出
#hive -e 'SET hive.cli.print.header=false; SELECT * FROM mmconsume_billevents_1d' | sed -e 's/\t/;/g' > /root/qwm/data/mmconsume_billevents_1d.csv
#删除es中mmconsume_billevents_test数据
curl -XDELETE http://$2:9200/$7
#将mmconsume_billevents_1d表的数据导入到es
spark-submit --class com.tipdm.scala.datasource.Hive2Elasticsearch --executor-memory 10g --total-executor-cores 20 --master spark://$3:7077 --jars /opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/spark/lib/elasticsearch-spark-13_2.10-6.3.2.jar $4 mmconsume_billevents_1d $2 $5 $6 $7 $8
#将mmconsume_billevents_1d.csv数据导入到es
#nohup /data/logstash-6.3.2/bin/logstash -f /root/qwm/mmconsume/mmconsume_billevents_1d.conf --path.data=/root/qwm/mmconsume/mmconsume_billevents_1d &
end_time=$(date +%Y-%m-%d-%H:%M:%S)
echo "任务结束运行时间: $end_time" >> $log_file