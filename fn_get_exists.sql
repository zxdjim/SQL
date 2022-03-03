use 8bet;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_get_exists`$$
CREATE FUNCTION `fn_get_exists`(source_delimiter varchar(200),source_char varchar(4000),target_char varchar(4000))
RETURNS bit DETERMINISTIC
label1:BEGIN
###  DEMO : SELECT fn_get_exists(',','abc,mmm,123','abc,123mmm|ddd'); 
###  first_delimiter  必填项 源字符串分隔符 如 ',' 必填项
###  source_char      必填项 源字符串   如'abc,mmm,123'
###  target_char      必填项 目标字符串 如'abc,123mmm|ddd'
###  函数说明:通过 source_delimiter 传入的字符把source_char分隔成N个子串,然后到target_char
###           进行匹配,如果都匹配到返回1,否则返回0
###  author : jim      date: 20191117

		DECLARE cnt_source int default 0;
		DECLARE i int default 1;
		DECLARE sTemp VARCHAR(4000) default '';
		DECLARE v_source_char VARCHAR(4000) default '';
		DECLARE v_target_char VARCHAR(4000) default '';
		
		IF length(trim(source_delimiter)) = 0 or source_delimiter is null  then
			 return 0;
			 LEAVE label1;
		END IF;
		
		IF (length(trim(source_char)) = 0 and length(trim(target_char)) = 0) or (source_delimiter is null and target_char is null)  then
			 return 1;
			 LEAVE label1;
		END IF;
		
		### 统计出分隔符在源字符串中的次数
	  SET cnt_source = (LENGTH(source_char) - LENGTH(REPLACE(source_char,source_delimiter,''))) / LENGTH(source_delimiter);
		
		set v_source_char = upper(trim(source_char));
		set v_target_char = upper(trim(target_char));
		set i = 1;
		while i <= cnt_source + 1 do
				### 从源表取字段到目标表中查找,找不到则说明列名不一致
				set sTemp = trim(substring_index(substring_index(v_source_char,',',i),',',-1));
				if locate(sTemp,v_target_char) = 0 then
					 return 0;
					 LEAVE label1;
				end if;			
				set i = i + 1;
		end while;
										
	###在目标串中都匹配到返回1,否则返回0									
  RETURN 1;
END $$
DELIMITER ;