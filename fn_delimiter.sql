use 8bet;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_delimiter`$$
CREATE FUNCTION `fn_delimiter`(v_delimiter varchar(200),target_char varchar(4000))
RETURNS varchar(4000) DETERMINISTIC
label1:BEGIN
###  DEMO : SELECT fn_delimiter(',','abc,ddd'); 
###  v_delimiter   第一个分隔符 如 '('
###  target_char   目标字符串
###  函数说明： 通过 v_delimiter,把'abc,ddd' -> abc ddd 2行
###  author : jim      date: 20200106

		DECLARE cnt_delimiter int default 0;
		declare i int default 1;
		DECLARE sTemp VARCHAR(4000) default '';
		
		IF length(trim(v_delimiter)) = 0  or v_delimiter is null then
		  	# return '温馨提示：传入的分隔符为空或为NULL!';
			 return target_char;
			 LEAVE label1;
		END IF;
		
		### 统计出分隔符在目标字符串中的次数
	  SET cnt_delimiter = (LENGTH(target_char) - LENGTH(REPLACE(target_char,v_delimiter,''))) / LENGTH(v_delimiter) ;
	
		IF cnt_delimiter = 0  then
			 #return '温馨提示：分隔符在目标字符串中不存在!请确认分隔符是否在目标字符串中。';
			 return target_char;
			 LEAVE label1;
		END IF;		

		 while i <= cnt_delimiter + 1 do
				SET sTemp = concat(sTemp,substring_index(substring_index(target_char,v_delimiter,i),v_delimiter,-1));	
				SET i = i + 1;	
		 end while;	
		 ##SET sTemp = concat(sTemp,substring_index(target_char, second_delimiter, IF(cnt_first>=cnt_second, -1, -(ABS(cnt_second-cnt_first) + 1)))); 	
				 
RETURN sTemp;
END $$
DELIMITER ;