DROP TABLE IF EXISTS media_index_3m;
CREATE TABLE media_index_3m (
    terminal_no STRING, phone_no STRING, duration STRING, station_name STRING, 
    origin_time STRING, end_time STRING, owner_code STRING, owner_name STRING, 
    vod_cat_tags STRING, resolution STRING, audio_lang STRING, region STRING, 
    res_name STRING, res_type STRING, vod_title STRING, category_name STRING, 
    program_title STRING, sm_name STRING, first_show_time STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073' 
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS mediamatch_userevent_3m;
CREATE TABLE mediamatch_userevent_3m (
    phone_no STRING, run_name STRING, run_time STRING, owner_name STRING, 
    owner_code STRING, open_time STRING, sm_name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS mediamatch_usermsg_3m;
CREATE TABLE mediamatch_usermsg_3m (
    terminal_no STRING, phone_no STRING, sm_name STRING, run_name STRING, 
    sm_code STRING, owner_name STRING, owner_code STRING, run_time STRING, 
    addressoj STRING, estate_name STRING, open_time STRING, force STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073' 
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS mmconsume_billevents_3m;
CREATE TABLE mmconsume_billevents_3m (
    terminal_no STRING, phone_no STRING, fee_code STRING, year_month STRING, 
    owner_name STRING, owner_code STRING, sm_name STRING, should_pay STRING, 
    favour_fee STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073' 
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS order_index_v3;
CREATE TABLE order_index_v3 (
    phone_no STRING, owner_name STRING, optdate STRING, prodname STRING, 
    sm_name STRING, offerid STRING, offername STRING, business_name STRING, 
    owner_code STRING, prodprcid STRING, prodprcname STRING, effdate STRING, 
    expdate STRING, orderdate STRING, cost STRING, mode_time STRING, 
    prodstatus STRING, run_name STRING, orderno STRING, offertype STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073' 
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/export/data/raw_data/media_index_test.csv' INTO TABLE media_index_3m;
LOAD DATA LOCAL INPATH '/export/data/raw_data/mediamatch_userevent.csv' INTO TABLE mediamatch_userevent_3m;
LOAD DATA LOCAL INPATH '/export/data/raw_data/mediamatch_usermsg.csv' INTO TABLE mediamatch_usermsg_3m;
LOAD DATA LOCAL INPATH '/export/data/raw_data/mmconsume_billevents.csv' INTO TABLE mmconsume_billevents_3m;
LOAD DATA LOCAL INPATH '/export/data/raw_data/order_index_test.csv' INTO TABLE order_index_v3;
