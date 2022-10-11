select *
from IP_BANK_ZXYH_pc;
select max(logid)
from IP_BANK_ZXYH_mobile a;
select * from IP_BANK_ZXYH_mobile order by logId desc;
truncate table IP_BANK_ZXYH_mobile;
select * from IP_BANK_ZXYH_mobile;
drop table crawl_zxyh;
select * from crawl_zxyh;

truncate table IP_BANK_ZXYH_PROCESSED;
select * from IP_BANK_ZXYH_PROCESSED


    select column_name,data_type
            from all_tab_columns
            where table_name = 'IP_BANK_ZGGDYH_PROCESSED'
select * from ip_bank_zggdyh_processed
