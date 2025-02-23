select table_owner, table_name, partition_name,
       partition_position, num_rows, high_value
from all_tab_partitions 
where table_name = 'CALL_LOG_MAST';

select owner, table_owner, table_name, index_name, status, uniqueness, visibility
from dba_indexes
where table_name like 'CALL_LOG_MAST_STG'
order by table_owner, table_name, index_name;


alter index CLP_TRANSACTIONAL.PK_CALL_LOG_MAST_STG rebuild;


-- 1. ADDING PARTITION TO ARCHIVAL TABLE - CLP_HISTORY.CALL_LOG_MAST_HIST
alter table CLP_HISTORY.CALL_LOG_MAST_HIST ADD partition SYS_P40938 VALUES LESS THAN (to_date(20220601, 'YYYYMMDD'));

-- 2. EXCHANGE PARTITION FROM CLP_TRANSACTIONAL.CALL_LOG_MAST TABLE TO CLP_TRANSACTIONAL.CALL_LOG_MAST_STG
alter table CLP_TRANSACTIONAL.CALL_LOG_MAST  EXCHANGE PARTITION SYS_P40938 WITH TABLE CLP_TRANSACTIONAL.CALL_LOG_MAST_STG WITHOUT VALIDATION update global indexes; ---- AFTER THIS STEP INDEX BECOME UNSABLE

-- 3. EXCHANGE PARTITION FROM CLP_TRANSACTIONAL.CALL_LOG_MAST_STG TABLE TO CLP_HISTORY.CALL_LOG_MAST_HIST
alter table CLP_HISTORY.CALL_LOG_MAST_HIST  EXCHANGE PARTITION SYS_P40938 WITH TABLE CLP_TRANSACTIONAL.CALL_LOG_MAST_STG WITHOUT VALIDATION update global indexes; ---- AFTER THIS STEP INDEX BECOME UNSABLE
	
-- 4. NUMBER OF ROWS MOVED FROM CLP_TRANSACTIONAL.CALL_LOG_MAST TO CLP_HISTORY.CALL_LOG_MAST_HIST
select  NVL(NUM_ROWS,0) from all_tab_partitions WHERE PARTITION_NAME = 'SYS_P40938' AND TABLE_NAME='CALL_LOG_MAST_HIST';

-- 5. DROPPING MAIN TABLE PARTITION CLP_TRANSACTIONAL.CALL_LOG_MAST
alter table CLP_TRANSACTIONAL.CALL_LOG_MAST drop partition SYS_P40938 update global indexes

----------------------------------------------------------------------------------------------------------------------------------------------------------------
--- #############################################################################################################################################################

-- 2
alter table CLP_TRANSACTIONAL.TRANSACTION_LOG  EXCHANGE PARTITION SYS_P20354 WITH TABLE CLP_TRANSACTIONAL.TRANSACTION_LOG_STG WITHOUT VALIDATION update global indexes
-- 2. Internally
insert /*+ RELATIONAL("TRANSACTION_LOG") NO_PARALLEL APPEND NESTED_TABLE_SET_SETID NO_REF_CASCADE */   into "CLP_TRANSACTIONAL"."TRANSACTION_LOG"  partition ("SYS_P20354") 
select /*+ RELATIONAL("TRANSACTION_LOG") NO_PARALLEL  */  *  from  NO_CROSS_CONTAINER ( "CLP_TRANSACTIONAL"."TRANSACTION_LOG" ) partition ("SYS_P20354") 
union all 
select /*+ RELATIONAL("TRANSACTION_LOG_STG") NO_PARALLEL  */  *  from  NO_CROSS_CONTAINER ( "CLP_TRANSACTIONAL"."TRANSACTION_LOG_STG" ) 
delete global indexes


-- 3
alter table CLP_HISTORY.TRANSACTION_LOG_HIST  EXCHANGE PARTITION SYS_P20354 WITH TABLE CLP_TRANSACTIONAL.TRANSACTION_LOG_STG WITHOUT VALIDATION update global indexes
-- 3. Internally
insert /*+ RELATIONAL("TRANSACTION_LOG_HIST") NO_PARALLEL APPEND NESTED_TABLE_SET_SETID NO_REF_CASCADE */   into "CLP_HISTORY"."TRANSACTION_LOG_HIST"  partition ("SYS_P20354") 
select /*+ RELATIONAL("TRANSACTION_LOG_HIST") NO_PARALLEL  */  *  from  NO_CROSS_CONTAINER ( "CLP_HISTORY"."TRANSACTION_LOG_HIST" ) partition ("SYS_P20354") 
union all 
select /*+ RELATIONAL("TRANSACTION_LOG_STG") NO_PARALLEL  */  *  from  NO_CROSS_CONTAINER ( "CLP_TRANSACTIONAL"."TRANSACTION_LOG_STG" )
delete global indexes


--alter table CLP_HISTORY.CALL_LOG_MAST_HIST drop partition SYS_P40938 update global indexes



-------------------------------------------------------------------------------------------------------------
-- 
-- EXCHANGE PARTITIION SYS_P42296 IN TRANSACTION_LOG TABLE (24 324 844 ROWS)

-- 0.A - PRE-REQUIREMENTS
-- Check indexes on TRANSACTION_LOG, TRANSACTION_LOG_STG and TRANSACTION_LOG_HIST. If necessary rebuild the indexes.
alter index CLP_TRANSACTIONAL.PK_TRAN_LOG_STG rebuild;
-- 0.B - Check the number of rows in the partition will be exchanged
select t.table_name, t.partition_name, t.high_value, t.num_rows, t.*
from all_tab_partitions t
where t.table_name = 'TRANSACTION_LOG'
order by t.partition_position;


-- 1 - ADDING PARTITION TO ARCHIVAL TABLE
alter table CLP_HISTORY.TRANSACTION_LOG_HIST ADD partition SYS_P42296 VALUES LESS THAN (to_date(20230201, 'YYYYMMDD'));

-- 2 - EXCHANGE PARTITION FROM CLP_TRANSACTIONAL TO STG
alter table CLP_TRANSACTIONAL.TRANSACTION_LOG EXCHANGE PARTITION SYS_P42296 WITH TABLE CLP_TRANSACTIONAL.TRANSACTION_LOG_STG PARALLEL(8) WITHOUT VALIDATION update global indexes; 

-- 2.A - REBUILD INDEXES
alter index CLP_TRANSACTIONAL.PK_TRAN_LOG_STG rebuild;

-- 3 - EXCHANGE PARTITION FROM STG TABLE TO CLP_HISTORY
alter table CLP_HISTORY.TRANSACTION_LOG_HIST EXCHANGE PARTITION SYS_P42296 WITH TABLE CLP_TRANSACTIONAL.TRANSACTION_LOG_STG PARALLEL(8)  WITHOUT VALIDATION update global indexes;

-- 4 - CHECK THE NUMBER OF ROWS MOVED FROM CLP_TRANSACTIONAL TO CLP_HISTORY
select NVL(NUM_ROWS,0) from all_tab_partitions WHERE PARTITION_NAME = 'SYS_P42296' AND TABLE_NAME='TRANSACTION_LOG_HIST';

-- 5 - DROPPING MAIN TABLE PARTITION CLP_TRANSACTIONAL.CALL_LOG_MAST
alter table CLP_TRANSACTIONAL.TRANSACTION_LOG drop partition SYS_P42296 update global indexes;

-- 
-------------------------------------------------------------------------------------------------------------

