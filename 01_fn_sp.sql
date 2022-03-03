use xyz;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_get_exists`$$
CREATE FUNCTION `fn_get_exists`(source_delimiter varchar(200),source_char varchar(4000),target_delimiter varchar(200),target_char varchar(4000))
RETURNS bit DETERMINISTIC
label1:BEGIN
###  DEMO : SELECT fn_get_exists(',','abc,mmm,123',',','abc,123,mmm,ddd'); 
###  source_delimiter  必填项 源字符串分隔符 如 ','
###  source_char      必填项 源字符串   如'abc,mmm,123'
###  target_delimiter 必填项 目标字符串分隔符 如 ','
###  target_char      必填项 目标字符串 如'abc,123,mmm,ddd'
###  函数说明:通过 source_delimiter和target_delimiter 传入的字符把source_char和target_char分隔成N个子串,
###           N个单子串进行一一匹配,如果都匹配到返回1,否则返回0
###  author : jim      date: 20191117

		DECLARE source_cnt int default 0;
		DECLARE target_cnt int default 0;
		DECLARE i int default 1;
		DECLARE j int default 1;
		DECLARE sum int default 0;
		DECLARE sTemp VARCHAR(4000) default '';
		DECLARE tTemp VARCHAR(4000) default '';
		DECLARE v_source_char VARCHAR(4000) default '';
		DECLARE v_target_char VARCHAR(4000) default '';
		
		IF length(trim(source_delimiter)) = 0 or source_delimiter is null  then
			 return 0;
			 LEAVE label1;
		END IF;
		
		IF (length(trim(source_char)) = 0 and length(trim(target_char)) = 0) or (source_char is null and target_char is null)  then
			 return 1;
			 LEAVE label1;
		END IF;

		### 统计出分隔符在源字符串中的次数
	  SET source_cnt = (LENGTH(source_char) - LENGTH(REPLACE(source_char,source_delimiter,''))) / LENGTH(source_delimiter) + 1;
		SET target_cnt = (LENGTH(target_char) - LENGTH(REPLACE(target_char,target_delimiter,''))) / LENGTH(target_delimiter) + 1;
		
		set v_source_char = upper(trim(source_char));
		set v_target_char = upper(trim(target_char));
		set i = 1;
		out_loop:loop
		    if i <= source_cnt then
				### 从源表取字段到目标表中查找,找不到则说明列名不一致
			  	 set sTemp = trim(substring_index(substring_index(v_source_char,source_delimiter,i),source_delimiter,-1));
					 set j = 1;
					 inner_loop:loop
		       if j <= target_cnt then
					     set tTemp = trim(substring_index(substring_index(v_target_char,target_delimiter,j),target_delimiter,-1));
							 if sTemp != tTemp and sTemp !=''  then
								  set sum = sum + 0;
							 else
						   	  set sum = sum + 1;
									LEAVE inner_loop;
							 end if;
							 if j = target_cnt then
							    LEAVE inner_loop;
							 end if;
							 set j = j + 1;		
					  end if;	 
					 end loop;
				IF i = source_cnt or sum = source_cnt THEN 
				   LEAVE out_loop; 
        END IF;
				set i = i + 1;					 
				end if;
		end loop;
										
	###在目标串中都匹配到返回1,否则返回0	
	IF sum = source_cnt THEN
     RETURN 1;
	ELSE
     RETURN 0;
	END IF;	 
	
END $$
DELIMITER ;

use xyz;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_get_table_fileds`$$
CREATE FUNCTION `fn_get_table_fileds`(v_table_schema varchar(50),v_table_name varchar(200),v_list_type char(3))
RETURNS varchar(4000) DETERMINISTIC
label1:BEGIN
###  DEMO : SELECT fn_get_table_fileds('xyz','t_pay_channel','AL'); 
###  v_table_schema    必填项 库名     如 'xyz' 
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
		 SET group_concat_max_len=102400;
	
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

use xyz;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_StrContent_delimiter`$$
CREATE FUNCTION `fn_StrContent_delimiter`(v_delimiter varchar(200),v_AddContent varchar(200),v_target_char varchar(4000),v_site bit)
RETURNS text DETERMINISTIC
label1:BEGIN
###  DEMO : SELECT fn_StrContent_delimiter('_', '1', '_41963_41964_',0)
###  v_delimiter     分隔符 如 '('
###  v_AddContent    要加的字符串
###  v_target_char   目标字符串
###  v_site          要加的字符串位于目标字符串位置, 0:前 1:后
###  函数说明：      通过在分隔符之后加的字符串，来达到实现修改目标字符串的功能
###  author : jim      date: 20191113

		DECLARE cnt_delimiter int default 0;
		declare i int default 1;
		DECLARE sTemp VARCHAR(4000) default '';
		DECLARE sReturn text default '';
		DECLARE v_site_new int default 0;
		
		IF length(trim(v_delimiter)) = 0 or v_delimiter is null  then
			 return v_target_char;
			 LEAVE label1;
		END IF;
		
		#### 默认位于目标串前面
		if v_site is not null then
		   set v_site_new = v_site;
		end if;
		
		### 统计出分隔符在目标字符串中的次数
	  SET cnt_delimiter = (LENGTH(v_target_char) - LENGTH(REPLACE(v_target_char,v_delimiter,''))) / LENGTH(v_delimiter) ;

		IF cnt_delimiter = 0   then
			 return v_target_char;
			 LEAVE label1;
		END IF;		
    
		 while i <= cnt_delimiter + 1 do
		    SET sTemp = substring_index(substring_index(v_target_char,v_delimiter,i),v_delimiter, -1); 
	      if v_site_new = 0 then			
				   SET sReturn = concat(sReturn, if(sTemp='','',v_AddContent), sTemp);
				ELSE
				   SET sReturn = concat(sReturn, sTemp, if(sTemp='','',v_AddContent));
		    end if;		
				SET i = i + 1;
			  if  i <= cnt_delimiter + 1 then
				   set sReturn = concat(sReturn,v_delimiter);
		    end if;	
		 end while;
					 
RETURN sReturn;
END $$
DELIMITER ;

use xyz;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_StrContent_DDelimiter`$$
CREATE FUNCTION `fn_StrContent_DDelimiter`(v_delimiter varchar(200),v_AddContent_1 varchar(500),v_AddContent_2 varchar(500),v_AddContent_3 varchar(500),v_target_char varchar(4000))
RETURNS text DETERMINISTIC
label1:BEGIN
###  DEMO : SELECT fn_StrContent_DDelimiter('_', 'sum(',') ','', '_abc_ddd_');
###  返回值为 _sum(abc) abc_sum(ddd) ddd_ 
###  v_delimiter     分隔符 如 '('
###  v_AddContent_1   要加的字符串1
###  v_AddContent_2   要加的字符串2
###  v_AddContent_3   要加的字符串3
###  v_target_char   目标字符串
###  函数说明：      通过在分隔符之后加的字符串，来达到实现修改目标字符串的功能
###  author : jim      date: 20191113

		DECLARE cnt_delimiter int default 0;
		declare i int default 1;
		DECLARE sTemp VARCHAR(4000) default '';
		DECLARE sReturn text default '';
		
		IF length(trim(v_delimiter)) = 0 or v_delimiter is null  then
			 return v_target_char;
			 LEAVE label1;
		END IF;
		
		if v_AddContent_1 is null then
		   set v_AddContent_1 = '';
		end if;
		if v_AddContent_2 is null then
		   set v_AddContent_2 = '';
		end if;
		if v_AddContent_3 is null then
		   set v_AddContent_3 = '';
		end if;
		
		### 统计出分隔符在目标字符串中的次数
	  SET cnt_delimiter = (LENGTH(v_target_char) - LENGTH(REPLACE(v_target_char,v_delimiter,''))) / LENGTH(v_delimiter) ;

		IF cnt_delimiter = 0   then
			 return v_target_char;
			 LEAVE label1;
		END IF;		
    
		 while i <= cnt_delimiter + 1 do
		    SET sTemp = substring_index(substring_index(v_target_char,v_delimiter,i),v_delimiter, -1); 	
			  SET sReturn = concat(sReturn, if(sTemp='','',v_AddContent_1), sTemp, if(sTemp='','',v_AddContent_2), sTemp, if(sTemp='','',v_AddContent_3));	
				SET i = i + 1;
			  if  i <= cnt_delimiter + 1 then
				   set sReturn = concat(sReturn,v_delimiter);
		    end if;	
		 end while;
					 
RETURN sReturn;
END $$
DELIMITER ;

use xyz;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_NumContent_delimiter`$$
CREATE FUNCTION `fn_NumContent_delimiter`(v_delimiter varchar(200),v_NumContent int,v_target_char text)
RETURNS text DETERMINISTIC
label1:BEGIN
###  DEMO : SELECT fn_NumContent_delimiter('_', 20, '_41963_4196_')
###  v_delimiter     分隔符 如 '('
###  v_NumContent    子串要加的数字
###  v_target_char   目标字符串
###  函数说明：  例如: 字符串:'_41963_4196_'  子串:41963 和 4196
###  v_NumContent，来达到实现修改目标字符串中各子串+v_NumContent的功能,
###   如果子串不是数字则可能异常
###  author : jim      date: 20191120

		DECLARE cnt_delimiter int default 0;
		declare i int default 1;
		DECLARE sTemp VARCHAR(500) default '';
		DECLARE iTemp int default 0;
		DECLARE sReturn text default '';
		
		IF length(trim(v_delimiter)) = 0 or v_delimiter is null or length(trim(v_NumContent)) = 0 or  v_NumContent is null then
			 return v_target_char;
			 LEAVE label1;
		END IF;
		
		### 统计出分隔符在目标字符串中的次数
	  SET cnt_delimiter = (LENGTH(v_target_char) - LENGTH(REPLACE(v_target_char,v_delimiter,''))) / LENGTH(v_delimiter) ;

		IF cnt_delimiter = 0   then
			 return v_target_char;
			 LEAVE label1;
		END IF;		
    
		 while i <= cnt_delimiter + 1 do
		    set sTemp = substring_index(substring_index(v_target_char,v_delimiter,i),v_delimiter,-1);
				IF sTemp != '' then
				   set iTemp = cast(sTemp as SIGNED); 
				END IF;
				SET sReturn = concat(sReturn, if(sTemp ='','',iTemp + v_NumContent));	
				SET i = i + 1;	
				if  i <= cnt_delimiter + 1 then
				   set sReturn = concat(sReturn,v_delimiter);
		        end if;	
		 end while;	
			 
RETURN sReturn;
END $$
DELIMITER ;

select '创建过程和函数成功,请进行下一步操作!' as result;