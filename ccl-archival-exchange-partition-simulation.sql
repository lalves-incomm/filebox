select * from dba_operations.ccl_archive_mgmt_ctl;
select max(step_number) from dba_operations.ccl_archive_mgmt_log;

--- Logs archival process
select * from dba_operations.ccl_archive_mgmt_log order by step_number desc;
select max(step_number) from dba_operations.ccl_archive_mgmt_log;


select l.step_number, l.object_name, l.step_name, l.step_run_status, 
       --(l.step_end_dttm - l.step_start_dttm) total_time,
       l.step_start_dttm,
       l.comments, l.*
from dba_operations.ccl_archive_mgmt_log l
where 1 = 1
  --and object_name = 'CALL_LOG_MAST'
  --and trunc(process_run_dttm) = to_date('2023-05-23','yyyy-mm-dd')
  --and process_name = 'ARCHIVE'
  and l.step_number > 3345
order by l.step_number desc;

--- control table
select object_name, (add_months(trunc(sysdate,'MM'),-retention_period)), 
       process_order_number, retention_period
from DBA_OPERATIONS.ccl_archive_mgmt_ctl;

select object_name, process_order_number, exclude_indicator, retention_period
from DBA_OPERATIONS.ccl_archive_mgmt_ctl;

-- exclude all tables
update DBA_OPERATIONS.ccl_archive_mgmt_ctl
set exclude_indicator = 'Y';

-- include only one table
update DBA_OPERATIONS.ccl_archive_mgmt_ctl
set exclude_indicator = 'N',
    retention_period = 35
where object_name = 'CALL_LOG_MAST';


---------------------------------------
-- partitions
select table_owner, table_name, partition_name,
       partition_position, num_rows, high_value
from all_tab_partitions 
where table_name = 'CALL_LOG_MAST';
-- table_owner = 'CLP_TRANSACTIONAL' 

select * from all_tab_partitions where table_owner = 'CLP_TRANSACTIONAL' and table_name = 'CALL_LOG_MAST';
select * from all_tab_partitions where table_owner = 'CLP_HISTORY' and table_name = 'CALL_LOG_MAST_HIST';

---- indexes
select owner, table_owner, table_name, index_name, status, uniqueness, visibility
from dba_indexes
where table_name like 'CALL_LOG_MAST%STG'
order by table_owner, table_name, index_name;

alter index CLP_TRANSACTIONAL.PK_CALL_LOG_MAST_STG rebuild;

-------------------------------------------------------
 -- calculate date string based on the retention period (in months)
select to_char(add_months(sysdate, -35),'YYYYMMDD') from dual;

  
select * from clp_transactional.call_log_mast partition(SYS_P28467);


----------------------------------------------------------------------
---- SIMULATION: ARCHIVE PROCESS
--- the high value column is a long datatype
declare
  p_run_date_in             dba_operations.CCL_ARCHIVE_MGMT_LOG.process_run_dttm%TYPE := sysdate;
  p_retention_period_in     dba_operations.CCL_ARCHIVE_MGMT_CTL.RETENTION_PERIOD%TYPE := 35;
  p_object_owner_in         all_tables.owner%TYPE := 'CLP_TRANSACTIONAL';
  p_object_name_in          dba_operations.CCL_ARCHIVE_MGMT_CTL.object_name%TYPE := 'CALL_LOG_MAST';
  
  l_long            LONG;
  l_high_value_date VARCHAR2(32767);
  l_date_str        VARCHAR2(20); --to store the date as string to be used in calculating age of data
begin
  -- calculate date string based on the retention period (in months)
  l_date_str := to_char(add_months(p_run_date_in, -p_retention_period_in), 'YYYYMMDD');

  for l_cur_part IN (SELECT partition_name,high_value,num_rows
                     FROM all_tab_partitions
                    WHERE table_name = p_object_name_in
                      AND partition_position != 1
                    ORDER BY partition_position)
  loop
    l_long := l_cur_part.high_value;
    
    --calculate the high value for the partition
    --needs special processing as the column is defined as LONG
    l_high_value_date := REPLACE(TRIM(substr(l_long,
                                             instr(l_long, '''', 1, 1) + 1,
                                             instr(l_long, ' ', 1, 2) -
                                             instr(l_long, '''', 1, 1))),
                                       '-',
                                       '');

    l_long := l_cur_part.high_value;
    if to_date(l_high_value_date, 'YYYYMMDD') < to_date(l_date_str, 'YYYYMMDD') then
      dbms_output.put_line('Archive the partitiion: ' || l_cur_part.partition_name || ' - ' || l_cur_part.high_value);
      
    end if;
    --dbms_output.put_line('Partition name: ' || l_cur_part.partition_name);
    --dbms_output.put_line('High value: ' || l_high_value_date);
  end loop;
end;


--- select the partitions to be archived
select p.table_owner, p.table_name, p.partition_name, 
       p.high_value,
       p.partition_position,
       p.num_rows, p.blocks,
       p.*
from all_tab_partitions p
where table_owner = 'CLP_TRANSACTIONAL' 
  and table_name = 'STATEMENTS_LOG'
order by p.partition_position;

select * from CLP_TRANSACTIONAL.STATEMENTS_LOG partition(FIRST_PARTITION) ORDER BY 1;

select count(*) from CLP_TRANSACTIONAL.STATEMENTS_LOG partition(FIRST_PARTITION);


select count(*) from CLP_TRANSACTIONAL.TRANSACTION_LOG partition(FIRST_PARTITION);
select count(*) from CLP_TRANSACTIONAL.STATEMENTS_LOG partition(FIRST_PARTITION);
select count(*) from CLP_HISTORY.TRANSACTION_LOG_HIST partition(FIRST_PARTITION);
select count(*) from CLP_HISTORY.STATEMENTS_LOG_HIST partition(FIRST_PARTITION);

/*
ALTER TABLE CLP_TRANSACTIONAL.STATEMENTS_LOG TRUNCATE PARTITION FIRST_PARTITION;


------------------------------------------------------------------------------------------------------------------------
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname     => 'CLP_TRANSACTIONAL',
        tabname     => 'STATEMENTS_LOG'
    );
END;

------------------------------------------------------------------------------------------------------------------------
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname     => 'CLP_HISTORY',
        tabname     => 'STATEMENTS_LOG_HIST'
    );
END;

--- REBUILDING UNUSABLE INDEXES (IMP doesn´t work well if there´s a unusable index in the destination table)
alter index CLP_HISTORY.IDX_STAN_HIST rebuild;
alter index CLP_HISTORY.UK_TRAN_LOG_HIST rebuild;
alter index CLP_HISTORY.IDX_CASHIER_ID_HIST rebuild;
alter index CLP_HISTORY.IDX_RESPONSE_ID_HIST rebuild;
alter index CLP_HISTORY.IDX_TRAN_LOG_INSDT_HIST rebuild;
alter index CLP_HISTORY.IDX_HISO_MESSAGE_TYPE_HIST rebuild;
alter index CLP_HISTORY.IDX_TOPUP_CARD_NUMBER_HIST rebuild;
alter index CLP_HISTORY.TRANSACTION_LOG_INDEX_HIST rebuild;
alter index CLP_HISTORY.IDX_TRANS_LOG_CUST_ENCR_HIST rebuild;
alter index CLP_HISTORY.IDX_TRAN_LOG_ACCOUNT_ID_HIST rebuild;
alter index CLP_HISTORY.TRANSACTION_LOG_INDEX_1_HIST rebuild;
alter index CLP_HISTORY.IDX_TRAN_LOG_NETWORK_RRN_HIST rebuild;


alter index CLP_TRANSACTIONAL.IDX_STAN rebuild;
alter index CLP_TRANSACTIONAL.PK_TRAN_LOG rebuild;
alter index CLP_TRANSACTIONAL.IDX_CASHIER_ID rebuild;
alter index CLP_TRANSACTIONAL.IDX_RESPONSE_ID rebuild;
alter index CLP_TRANSACTIONAL.IDX_HISO_MESSAGE_TYPE rebuild;
alter index CLP_TRANSACTIONAL.IDX_TOPUP_CARD_NUMBER rebuild;
alter index CLP_TRANSACTIONAL.IDX_TRAN_LOG_INS_DATE rebuild; --ALTER INDEX REBUILD PARTITION)
alter index CLP_TRANSACTIONAL.TRANSACTION_LOG_INDEX rebuild;
alter index CLP_TRANSACTIONAL.IDX_TRAN_LOG_ACCOUNT_ID rebuild;
alter index CLP_TRANSACTIONAL.TRANSACTION_LOG_INDEX_1 rebuild;
alter index CLP_TRANSACTIONAL.IDX_TRAN_LOG_NETWORK_RRN rebuild;


alter index CLP_TRANSACTIONAL.PK_STMT_LOG rebuild;
alter index CLP_TRANSACTIONAL.UK_STMT_LOG rebuild;
alter index CLP_TRANSACTIONAL.IDX_STATE_LOG_TRAN_SQID rebuild;
alter index CLP_TRANSACTIONAL.IDX_STAT_LOG_ACCOUNT_ID rebuild;
alter index CLP_TRANSACTIONAL.IDX_STAT_LOG_CARD_NUM_HSH_FK rebuild;
*/