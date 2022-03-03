use xyz;
DROP PROCEDURE IF EXISTS sp_table_optimize;
delimiter $$
CREATE PROCEDURE sp_table_optimize(in_over_mb int)
label1:begin
## DEMO: call sp_table_optimize(10);
## in_over_mb:    超过多大MB  必填项  如 10,表内的碎片超过这个值就进行表的优化

 IF in_over_mb is null  or  in_over_mb <= 0  then
	  select '温馨提示：传入的参数不可为NULL和非正整数的值' as result;
    leave label1;
 END IF;
 
		CREATE TABLE IF NOT EXISTS t_table_optimize_log(
		        TID  int not null auto_increment primary key comment '主键',
						TMEMO          varchar(50) default '' comment '备注',
						TABLE_NAME     varchar(50) default '' comment '库名+表名',
						TABLE_ENGINE   varchar(20) default '' COMMENT '存储引擎名称',
						TABLE_TYPE     varchar(20) default '' comment '表类型,系统表,视图还是用户表',
						TABLE_ROWS     int default NULL comment '表中当时的记录数',
						TB_DATA_SIZE   varchar(20) default '' comment '表记录空间大小',
						TB_IDX_SIZE    varchar(20) default '' comment '表索引空间大小',
						TOTAL_SIZE     varchar(20) default '' comment '表总空间大小',
						TB_INDX_RATE   varchar(20) default '' comment '表记录空间大小',
						TB_DATA_FREE   varchar(20) default '' comment '表中碎片空间大小',
						TB_FREE_RATE   varchar(20) default '' comment '表中碎片所占总空间比例',
						CREATION_BY    varchar(30) default 'sys' comment '创建人',
            CREATION_TIME  datetime default CURRENT_TIMESTAMP comment '创建时间'
						);

    ### START optimize table
	  INSERT INTO t_table_optimize_log(TMEMO) VALUES('START tables optimize ...');
	begin	
	  declare done int default false;	
		declare V_TABLE_NAME     varchar(50) default '';
		declare V_TABLE_ENGINE   varchar(20) default '';
    declare V_TABLE_TYPE     varchar(20) default '';
		declare V_TABLE_ROWS     int default NULL;
		declare V_TB_DATA_SIZE   varchar(20) default '';
    declare V_TB_IDX_SIZE    varchar(20) default '';
		declare V_TOTAL_SIZE     varchar(20) default '';
		declare V_TB_INDX_RATE   varchar(20) default '';
    declare V_TB_DATA_FREE   varchar(20) default '';
		declare V_TB_FREE_RATE   varchar(20) default '';
		
		DECLARE my_cur CURSOR for SELECT CONCAT(table_schema, '.', table_name)                    AS  TABLE_NAME
																		,engine                                                   AS  TABLE_ENGINE 
																		,table_type                                               AS  TABLE_TYPE
																		,table_rows                                               AS  TABLE_ROWS
																		,CONCAT(ROUND(data_length  / ( 1024 * 1024), 2), 'M')     AS  TB_DATA_SIZE 
																		,CONCAT(ROUND(index_length / ( 1024 * 1024), 2), 'M')     AS  TB_IDX_SIZE 
																		,CONCAT(ROUND((data_length + index_length ) 
																					/ ( 1024 * 1024 ), 2), 'M')                         AS  TOTAL_SIZE
																		,CASE WHEN  data_length =0 THEN 0
																					ELSE  ROUND(index_length / data_length, 2) END      AS  TB_INDX_RATE
																	,CONCAT(ROUND( data_free / 1024 / 1024,2), 'MB')            AS  TB_DATA_FREE 
																	,CASE WHEN (data_length + index_length) = 0 THEN 0
																					 ELSE ROUND(data_free/(data_length + index_length),2) 
																	 END                                                        AS  TB_FREE_RATE
															FROM information_schema.TABLES  
															WHERE  TABLE_SCHEMA in ('xyz','xyz_member1','xyz_member2','xyz_member3') 
															       AND UPPER(TRIM(table_type))='BASE TABLE' and ROUND(DATA_FREE/1024/1024,2) >=in_over_mb  ORDER BY data_free DESC;
	  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;
		open my_cur;
	  read_loop:loop
		FETCH my_cur INTO  V_TABLE_NAME,V_TABLE_ENGINE,V_TABLE_TYPE,V_TABLE_ROWS,V_TB_DATA_SIZE,V_TB_IDX_SIZE,V_TOTAL_SIZE,V_TB_INDX_RATE,V_TB_DATA_FREE,V_TB_FREE_RATE;
				IF done then
					 leave read_loop;
				END IF;
				### before optimize table
				INSERT INTO t_table_optimize_log(TMEMO,TABLE_NAME,TABLE_ENGINE,TABLE_TYPE,TABLE_ROWS,TB_DATA_SIZE,TB_IDX_SIZE,TOTAL_SIZE,TB_INDX_RATE,TB_DATA_FREE,TB_FREE_RATE)
					VALUES('optimize before',V_TABLE_NAME,V_TABLE_ENGINE,V_TABLE_TYPE,V_TABLE_ROWS,V_TB_DATA_SIZE,V_TB_IDX_SIZE,V_TOTAL_SIZE,V_TB_INDX_RATE,V_TB_DATA_FREE,V_TB_FREE_RATE);

        ### Running optimize table
				SET @SQL = CONCAT('ALTER TABLE ',V_TABLE_NAME,' ENGINE = INNODB;');
				PREPARE STMT FROM @SQL;
				EXECUTE STMT;	
				DEALLOCATE PREPARE STMT;
				
				### after optimize table
				INSERT INTO t_table_optimize_log(TMEMO,TABLE_NAME,TABLE_ENGINE,TABLE_TYPE,TABLE_ROWS,TB_DATA_SIZE,TB_IDX_SIZE,TOTAL_SIZE,TB_INDX_RATE,TB_DATA_FREE,TB_FREE_RATE)
				SELECT 'optimize after',CONCAT(table_schema, '.', table_name)                    AS  TABLE_NAME
																						,engine                                                   AS  TABLE_ENGINE 
																						,table_type                                               AS  TABLE_TYPE
																						,table_rows                                               AS  TABLE_ROWS
																						,CONCAT(ROUND(data_length  / ( 1024 * 1024), 2), 'M')     AS  TB_DATA_SIZE 
																						,CONCAT(ROUND(index_length / ( 1024 * 1024), 2), 'M')     AS  TB_IDX_SIZE 
																						,CONCAT(ROUND((data_length + index_length ) 
																									/ ( 1024 * 1024 ), 2), 'M')                         AS  TOTAL_SIZE
																						,CASE WHEN  data_length =0 THEN 0
																									ELSE  ROUND(index_length / data_length, 2) END      AS  TB_INDX_RATE
																					,CONCAT(ROUND( data_free / 1024 / 1024,2), 'MB')            AS  TB_DATA_FREE 
																					,CASE WHEN (data_length + index_length) = 0 THEN 0
																									 ELSE ROUND(data_free/(data_length + index_length),2) 
																					 END                                                        AS  TB_FREE_RATE
																			FROM information_schema.TABLES  
																			WHERE  TABLE_SCHEMA in ('xyz','xyz_member1','xyz_member2','xyz_member3') 
																						 AND UPPER(TRIM(table_type))='BASE TABLE' and CONCAT(table_schema, '.', table_name) = V_TABLE_NAME;
	  end loop;
	  close my_cur;		
		
	end;	
	
	### FINISH optimize table
	INSERT INTO t_table_optimize_log(TMEMO) VALUES('FINISH tables optimize !!!');
	
end;
$$
delimiter ;

call xyz.sp_table_optimize(10);