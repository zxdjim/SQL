create database if not exists my_tools;
use my_tools;

drop table if exists `t_rebuild_next_table_init`;
CREATE TABLE if not exists `t_rebuild_next_table_init` (
  `TID` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `SCHEMA_NAME` varchar(50) NOT NULL COMMENT '库名',
  `TABLE_NAME` varchar(200) NOT NULL COMMENT '表名',
  `is_type`   bit NOT NULL default b'0' COMMENT '表类型:0-按天,1-按月',
  `is_partition` bit NOT NULL DEFAULT b'0' COMMENT '是否分区表 0:否 1:是',
  `partition_field` varchar(50) DEFAULT NULL COMMENT '分区字段名称(非分区表此值无意义)',
  `is_enable`   bit NOT NULL default b'1' COMMENT '是否启用: 0-否,1-是',  
  `CREATION_BY` varchar(30) DEFAULT 'sys' COMMENT '创建人',
  `CREATION_TIME` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`TID`),
  KEY `idx_trnti` (`SCHEMA_NAME`,`TABLE_NAME`) USING BTREE
) ENGINE=InnoDB COMMENT='需要创建次日/月的配置表';

#### 按天分
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_game_record', 'game_record_cp', 0, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_game_record', 'game_record_dj', 0, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_game_record', 'game_record_dy', 0, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_game_record', 'game_record_qp', 0, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_game_record', 'game_record_ty', 0, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_game_record', 'game_record_zr', 0, 'sys', now());

#### 按月分
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_game_api', 'ob_game_transfer', 1, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_account', 'ob_discount', 1, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_account', 'ob_account_detail', 1, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_account', 'ob_order', 1, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_account', 'ob_user_bill_detatil', 1, 'sys', now());
INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_player',  'ob_login_log', 1, 'sys', now());

INSERT INTO `t_rebuild_next_table_init`(`SCHEMA_NAME`, `TABLE_NAME`, `is_type`, `CREATION_BY`, `CREATION_TIME`) VALUES ('ob_account',  'activity_visit_data', 1, 'sys', now());

use my_tools;
DELIMITER $$
drop procedure if exists sp_rebuild_next_table$$
create procedure sp_rebuild_next_table()
begin
### DEMO: call sp_rebuild_next_table;
### 调用则生成 t_rebuild_next_table_init 配置表 次日/月的表结构

		declare done int default false;	
		declare v_schema_name      varchar(50);
		declare v_table_name       varchar(100);
		declare v_target_table     varchar(150) default '';	 
        declare vi_max_tab         varchar(8);	###上一日/月的后缀数字 	
		declare v_next_ymd         varchar(8) default DATE_FORMAT(NOW() + INTERVAL 1 day, '%Y%m%d'); ###次日的后缀数字
		declare v_next_month       varchar(6) default DATE_FORMAT(NOW() + INTERVAL 1 MONTH, '%Y%m'); ###次月的后缀数字
		declare v_next_new         varchar(8);                                                       ###次日/月的后缀数字,合并上面2个变量
		
		declare v_is_type          bit;   ## 表类型:0-按天,1-按月
		declare v_is_partition     bit;   ## 是否分区表 0-否,1-是
		declare v_partition_field  varchar(50);  ## 分区字段名称(非分区表此值无意义) 

        declare i_next_days        tinyint    default day(last_day(NOW() + INTERVAL 1 MONTH));  ## 次月的最大天数
		declare i                  tinyint    default 1;                                        ## 天数变量
		declare v_current_date     int;

	
		DECLARE my_cur CURSOR for SELECT schema_name,table_name,is_type,is_partition,partition_field FROM t_rebuild_next_table_init where is_enable=1;
	  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;   
		
	  open my_cur;
		read_loop:loop
	  fetch my_cur into v_schema_name,v_table_name,v_is_type,v_is_partition,v_partition_field;
				IF done then
					leave read_loop;
				END IF;

                IF DAY(now()) < 20 and v_is_type + 0 = 1 then 	
				   SELECT CONCAT('温馨提示:每月20日(含)以后才开始重建次月表!') AS result;
				   ITERATE read_loop;				  
                END IF;
				
				SET v_target_table = concat(trim(v_schema_name),'.',trim(v_table_name));
				
				IF v_is_type + 0 = 0 then
					SELECT right(max(TABLE_NAME),8) into vi_max_tab FROM information_schema.tables where TABLE_SCHEMA = v_schema_name 
						 and TABLE_NAME like concat(v_table_name,'_20%') and right(TABLE_NAME,8) between 20200101 and 20991231 and right(TABLE_NAME,8) < v_next_ymd;
					set v_next_new = v_next_ymd;
				ELSE
					SELECT right(max(TABLE_NAME),6) into vi_max_tab FROM information_schema.tables where TABLE_SCHEMA = v_schema_name 
						 and TABLE_NAME like concat(v_table_name,'_20%') and right(TABLE_NAME,6) between 202001 and 209912 and right(TABLE_NAME,6) < v_next_month;
                    set v_next_new = v_next_month;
                END IF;	

                IF vi_max_tab is null then 	
				   SELECT CONCAT('温馨提示:库表',v_target_table,'_(后接6或8位数字)的表不存在,被跳过!') AS result;
				   ITERATE read_loop;				  
                END IF;
				
				### 此处不到tables表中去查记录数,是因为性能元字典视图默认刷新周期是一天,严重滞后,所以要实时去查询表中具体的数据量
				set @v_cnt = 0;
				select count(*) into @v_cnt from information_schema.tables where TABLE_SCHEMA=v_schema_name and TABLE_NAME=concat(v_table_name,'_',v_next_new);
				
				IF @v_cnt > 0 then 
				    set @v_cnt = 0;
					SET @SQL = CONCAT('select count(*) into @v_cnt from ',v_target_table,'_',v_next_new,';');
					PREPARE STMT FROM @SQL;
					EXECUTE STMT;	
					DEALLOCATE PREPARE STMT;
					
					IF @v_cnt > 0 THEN
					   SELECT CONCAT('温馨提示:重建次日/月表失败,原因是 【',v_target_table,'_',v_next_new,'】表记录不为空,不能删除后重建!') AS result;
					   ITERATE read_loop;
					END IF;
				
				END IF;
				
				SET @SQL = CONCAT('drop table if exists ',v_target_table,'_',v_next_new,';');
				PREPARE STMT FROM @SQL;
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT;	
					
				SET @SQL = CONCAT('create table ',v_target_table,'_',v_next_new,' like ',v_target_table,'_',vi_max_tab,';');
				PREPARE STMT FROM @SQL;
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT;				

				### 是分区表时自动增加下月的分区
				IF v_is_partition + 0 = 1 and v_is_type + 0 = 1 and v_partition_field is not null and length(trim(v_partition_field))>0 then

					## 先删除表分区,后面再重新创建
					set @SQL = CONCAT('ALTER TABLE ',v_target_table,'_',v_next_new,' remove partitioning;');
					prepare stmt from @SQL;
					execute stmt; 
					deallocate prepare stmt;
					
				   SET @SQL = CONCAT('ALTER TABLE ',v_target_table,'_',v_next_new,' PARTITION BY RANGE (',v_partition_field,') (');
				   SET i = 1;
				   for_loop:loop 
						 IF i > i_next_days then
								LEAVE for_loop;
						 END IF;
						 
						 ### 构造当前日期
						 SET v_current_date = IF(LENGTH(i)=2,concat(v_next_new,i),concat(v_next_new,'0',i));
						 
					     ### 构造当前日期的分区语句
					     SET @SQL = CONCAT(@SQL, ' PARTITION p_',v_current_date,' VALUES LESS THAN (',v_current_date + 1,') ENGINE = InnoDB,');
					     SET i = i + 1;

				   end loop;

				   SET @SQL = CONCAT(left(@SQL,length(@SQL) - 1),' );');
				   PREPARE stmt FROM @SQL;
				   EXECUTE stmt;
				   DEALLOCATE PREPARE stmt;
				END IF;
				
				## 如果游标中间有查询结果为NULL,则会自动设置 done = 1 或TRUE,导致游标提前结束循环.所以最后要重新设置一次为FALSE
			  SET done = false;
		end loop;		
	  close my_cur;
	  
end $$
DELIMITER ;

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

## replication_connection_status与replication_connection_configuration 表相比，其表中的记录变更更频繁
## 由于维护导致STOP SLAVE等操作,前者更敏感,可能导致查询出来的从库成了主库,从而执行了此过程(导致主从同步异常)

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

DELIMITER $$
drop EVENT IF EXISTS `se_rebulid_next_table`;
CREATE EVENT  `se_rebulid_next_table`
ON SCHEDULE EVERY 1 day  #执行周期，还有天、月等等 
STARTS concat(date_format(now(),'%Y-%m-%d'),' 23:59:00')
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Creating next tables'
DO BEGIN

	select fn_get_is_master()+0 into @is_master;
    if @is_master = 1 then 
       CALL sp_rebuild_next_table;
    else
	   select '当前是从库不需要执行此过程!' result;
    end if;		
END$$
DELIMITER ;
