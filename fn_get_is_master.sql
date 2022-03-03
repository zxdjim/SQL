use my_tools;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_get_is_master`$$
CREATE FUNCTION `fn_get_is_master`()
RETURNS bit DETERMINISTIC
BEGIN
###  DEMO : SELECT fn_get_is_master(); 
###  此函数用于判断当前服务器是否主库 返回值 0:否  1:是
###  author : baider      date: 2021-06-25

	## 以下2张关于复制主从同步的表位于 performance_schema
	## replication_connection_status          此表记录的是从库IO线程的连接状态信息
	## replication_connection_configuration   此表记录从库用于连接到主库的配置参数,该表中存储的配置信息在执行change master语句时会被修改
	
	## 与replication_connection_configuration 表相比，replication_connection_status表中的记录变更更频繁
	## 由于维护导致STOP SLAVE等操作,前者更敏感,导致查询出来的从库成了主库,从而执行了此过程(导致主从同步异常)
	select count(*) into @is_slave	from performance_schema.replication_connection_configuration;
    if @is_slave = 0 then 
       ### 主库
	   return 1;
    else
	   ### 从库
	   return 0;
    end if;	
	
END $$
DELIMITER ;