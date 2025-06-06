SELECT COUNT(1) 
FROM clp_order.IH_VENDOR_FILE_STATUS 
WHERE PROXY_NUMBER=?


these 3 query also added as part of  reprocessing check 

1
select TO_CHAR (c.EXPIRY_DATE,'YYMM') as expDate 
from clp_transactional.card c, 
	 clp_configuration.product p
where c.proxy_number = ? 
  and c.product_id = p.product_id 
order by c.INS_DATE desc
 
2
select clp_order.fn_dmaps_main(first_name) firstName,
	   clp_order.fn_dmaps_main(last_name) lastName 
from clp_order.customer_profile 
where customer_code = ?
 
3
select clp_order.fn_dmaps_main(pin_code) 
from clp_order.address 
where address_id = ?