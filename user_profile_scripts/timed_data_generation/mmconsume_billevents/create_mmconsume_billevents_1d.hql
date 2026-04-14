drop table if exists mmconsume_billevents_1d;
create table mmconsume_billevents_1d as select terminal_no,phone_no,fee_code,concat(add_months(year_month,3-1),' ','00:00:00')as year_month,owner_name,owner_code,sm_name,should_pay,favour_fee from mmconsume_billevents;
