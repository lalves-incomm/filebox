select min(transaction_sqid) from clp_transactional.statements_log;

select t.ins_date, t.* from clp_transactional.statements_log t where t.ins_date = to_date('09052004','ddmmyyyy');


select * 
from clp_transactional.statements_log
order  by transaction_sqid
fetch  first 40 rows only;

-------------------------------------------------------
insert into clp_transactional.statements_log
( INS_DATE
,TRANSACTION_SQID
,AVAIL_CLOSING_BALANCE
,ACCOUNT_PURSE_ID
,ACCOUNT_ID
,MERCHANT_CITY
,BUSINESS_DATE
,CARD_NUM_HASH
,CREDIT_DEBIT_COUNT
,AVAIL_OPENING_BALANCE
,RRN
,CLOSING_BALANCE
,AUTH_ID
,LAST_UPD_DATE
,TO_PURSE_ID
,TRANSACTION_TIME
,CREDIT_DEBIT_FLAG
,TRANSACTION_NARRATION
,STORE_ID
,INS_TIME_STAMP
,RECORD_SEQ
,OPENING_BALANCE
,FEE_FLAG
,TRANSACTION_CODE
,TRANSACTION_DATE
,CARD_LAST4DIGIT
,TO_ACCOUNT_ID
,PRODUCT_ID
,MERCHANT_NAME
,TRANSACTION_AMOUNT
,PURSE_ID
,MERCHANT_STATE
,DELIVERY_CHANNEL
,SOURCE_DESCRIPTION
,CARD_NUM_ENCR
)
select * from (
select to_date('09032004', 'ddmmyyyy') as INS_DATE, 
       rownum as TRANSACTION_SQID,
AVAIL_CLOSING_BALANCE
,ACCOUNT_PURSE_ID
,ACCOUNT_ID
,MERCHANT_CITY
,BUSINESS_DATE
,CARD_NUM_HASH
,CREDIT_DEBIT_COUNT
,AVAIL_OPENING_BALANCE
,RRN +1
,CLOSING_BALANCE
,AUTH_ID
,LAST_UPD_DATE
,TO_PURSE_ID
,TRANSACTION_TIME
,CREDIT_DEBIT_FLAG
,TRANSACTION_NARRATION
,STORE_ID
,INS_TIME_STAMP
,RECORD_SEQ
,OPENING_BALANCE
,FEE_FLAG
,TRANSACTION_CODE
,TRANSACTION_DATE
,CARD_LAST4DIGIT
,TO_ACCOUNT_ID
,PRODUCT_ID
,MERCHANT_NAME
,TRANSACTION_AMOUNT
,PURSE_ID
,MERCHANT_STATE
,DELIVERY_CHANNEL
,SOURCE_DESCRIPTION
,CARD_NUM_ENCR
from clp_transactional.statements_log partition(SYS_P20438)
cross  join (  
    select * from dual 
    connect by level <= 10)
    )
where TRANSACTION_SQID between 0 and 20;