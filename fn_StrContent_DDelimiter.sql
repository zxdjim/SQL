use 8bet;
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