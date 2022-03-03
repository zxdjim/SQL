use 8bet;
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