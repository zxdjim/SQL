USE 8bet;
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