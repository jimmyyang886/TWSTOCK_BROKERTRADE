# for speed up import database
SET GLOBAL innodb_flush_log_at_trx_commit=2;
show engines;
show variables like '%buffer%';
SELECT * FROM twstock.broker_transaction where Date='2020-09-26';

# for delete 
SET GLOBAL innodb_buffer_pool_size=402653184;
SET SQL_SAFE_UPDATES = 0;
DELETE from broker_transaction Where Date='2020-09-26';

show variables like '%buffer%';


SELECT * FROM twstock.broker_transaction;
show full processlist;
SELECT * FROM information_schema.INNODB_TRX;

show global variables like 'max_allowed_packet';

show variables like '%timeout%';


