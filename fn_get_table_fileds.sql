use 8bet;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_get_table_fileds`$$
CREATE FUNCTION `fn_get_table_fileds`(v_table_schema varchar(50),v_table_name varchar(200),v_list_type char(3))
RETURNS varchar(4000) DETERMINISTIC
label1:BEGIN
###  DEMO : SELECT fn_get_table_fileds('8bet','t_pay_channel','AL'); 
###  v_table_schema    必填项 库名     如 '8bet' 
###  v_table_name      必填项 表名     如't_pay_channel'
###  v_list_type       必填项 列表类型 如 'P/A/NAL/AL/NATL/ATL'
###  author : jim      date: 20191118

			declare v_PField                varchar(50);    ##主键列
			declare v_AField                varchar(50);    ##自增键列
		  declare v_NAFields_list         varchar(4000);  ##非主键列表
		  declare v_AFields_list          varchar(4000);  ##含主键列表
			declare v_NAFields_type_list    varchar(4000);  ##含非主键且带数据类型列表 			
			declare v_AFields_type_list     varchar(4000);  ##含主键且带数据类型列表 
					
    IF v_table_schema IS NULL OR v_table_name IS NULL OR v_list_type IS NULL 
		   OR LENGTH(TRIM(v_table_schema)) = 0 OR LENGTH(TRIM(v_table_name)) = 0 OR LENGTH(TRIM(v_list_type)) = 0 then
			 RETURN '所有参数不可为NULL或空串!!!';
			 LEAVE label1;
    end if;  
		
		 IF UPPER(TRIM(v_list_type)) NOT IN ('P','A','NAL','AL','NATL','ATL') then
			 RETURN '第三个参数只可为P/A/NAL/AL/NATL/ATL 六种类型中的一种,请确认无误后再尝试!!!';
			 LEAVE label1;
    end if; 
		
			### 设置 group_concat的最大值,原值为:1024过小和sql_mode
		 set session sql_mode='';
		 SET session group_concat_max_len=102400;
	
		select UPPER(GROUP_CONCAT(if(column_key='PRI',COLUMN_NAME,null) order by ORDINAL_POSITION)) PField,
		       UPPER(GROUP_CONCAT(if(extra='auto_increment',COLUMN_NAME,null) order by ORDINAL_POSITION)) AField,
					 UPPER(GROUP_CONCAT(if(extra='auto_increment',null,COLUMN_NAME) order by ORDINAL_POSITION)) NAFields_list,
					 UPPER(GROUP_CONCAT(COLUMN_NAME order by ORDINAL_POSITION)) AFields_list,
					 UPPER(GROUP_CONCAT(if(upper(data_type) in ('TINYINT','SMALLINT','MEDIUMINT','INT','BIGINT'),CONCAT(if(extra='auto_increment',null,COLUMN_NAME),' ',data_type),
								 CONCAT(if(extra='auto_increment',null,COLUMN_NAME),' ',column_type))  order by ORDINAL_POSITION)) NAFields_type_list,
					 UPPER(GROUP_CONCAT(if(upper(data_type) in ('TINYINT','SMALLINT','MEDIUMINT','INT','BIGINT'),CONCAT(COLUMN_NAME,' ',data_type),
								 CONCAT(COLUMN_NAME,' ',column_type))  order by ORDINAL_POSITION)) AFields_type_list								 
					 into v_PField,v_AField,v_NAFields_list,v_AFields_list,v_NAFields_type_list,v_AFields_type_list 
			from information_schema.`COLUMNS` 		
			where TABLE_SCHEMA = TRIM(v_table_schema) and table_name = TRIM(v_table_name);
	
	IF upper(TRIM(v_list_type)) = 'P' THEN
	   RETURN IFNULL(v_PField,'ERROR: NO PRIMARY KEY');
	ELSEIF upper(TRIM(v_list_type)) = 'A' THEN
	   RETURN IFNULL(v_AField,'ERROR: NO auto_increment');	 
	ELSEIF upper(TRIM(v_list_type)) = 'NAL' THEN
	   RETURN  v_NAFields_list;
  ELSEIF upper(TRIM(v_list_type)) = 'AL' THEN		
     RETURN  v_AFields_list;	
  ELSEIF upper(TRIM(v_list_type)) = 'NATL' THEN	
     RETURN  v_NAFields_type_list;	
  ELSEIF upper(TRIM(v_list_type)) = 'ATL' THEN		 	
     RETURN  v_AFields_type_list;	
  END IF;

END $$
DELIMITER ;