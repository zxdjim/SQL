use 8bet;
DELIMITER $$
DROP FUNCTION IF EXISTS  `fn_cancel_delimiter`$$
CREATE FUNCTION `fn_cancel_delimiter`(first_delimiter varchar(200),second_delimiter varchar(200),target_char varchar(4000))
RETURNS varchar(4000) DETERMINISTIC
label1:BEGIN
###  DEMO : SELECT fn_cancel_delimiter('(',')','abc,ddd(500)'); 
###  first_delimiter   第一个分隔符 如 '('
###  second_delimiter  第二个分隔符 如 ')'
###  target_char       目标字符串
###  函数说明： 通过 first_delimiter+second_delimiter(包含此2个分隔符中间的内容比如 (500),(255))
###  作为一个整体进行分隔，2个分隔符也可以只传一个,也即转换为单分隔符
###  author : jim      date: 20191112

		DECLARE cnt_first int default 0;
		DECLARE cnt_second int default 0;
		declare i int default 1;
		DECLARE sTemp VARCHAR(4000) default '';
		
		IF length(trim(first_delimiter)) = 0 or length(trim(second_delimiter)) = 0 or first_delimiter is null or second_delimiter is null then
		  	# return '温馨提示：传入的2个分隔符为空或为NULL!';
			 return target_char;
			 LEAVE label1;
		END IF;
		
		### 统计出第一和第二个分隔符在目标字符串中的次数
	  SET cnt_first = (LENGTH(target_char) - LENGTH(REPLACE(target_char,first_delimiter,''))) / LENGTH(first_delimiter) ;
    SET cnt_second = (LENGTH(target_char) - LENGTH(REPLACE(target_char,second_delimiter,''))) / LENGTH(second_delimiter) ;

	
		IF cnt_first = 0 or cnt_second = 0  then
			 #return '温馨提示：至少1个分隔符在目标字符串中没有发现！请保证2个字符串都出现在目标字符串中。';
			 return target_char;
			 LEAVE label1;
		END IF;
		
		IF upper(first_delimiter) = upper(second_delimiter) then
			 #RETURN '温馨提示：2个分隔符一样！ 请确认2个分隔符不一样。';
			 return target_char;
			 LEAVE label1;
		END IF; 

		 while i <= LEAST(cnt_first,cnt_second) do
				SET sTemp = concat(sTemp,substring_index(substring_index(target_char,first_delimiter,i),second_delimiter,-1));	
				SET i = i + 1;	
		 end while;	
		 SET sTemp = concat(sTemp,substring_index(target_char, second_delimiter, IF(cnt_first>=cnt_second, -1, -(ABS(cnt_second-cnt_first) + 1)))); 	
				 
RETURN sTemp;
END $$
DELIMITER ;