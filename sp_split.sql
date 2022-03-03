use 8bet;
DELIMITER $$
DROP PROCEDURE IF EXISTS  `sp_split`$$
CREATE PROCEDURE `sp_split`(v_delimiter varchar(200),target_char varchar(4000))
label1:BEGIN
###  DEMO : call sp_split(',','abc,ddd'); 
###  v_delimiter   第一个分隔符 如 '('
###  target_char   目标字符串
###  说明： 通过 v_delimiter,把'abc,ddd' -> abc ddd 2行
###  author : jim      date: 20200106

		DECLARE cnt_delimiter int default 0;
		declare i int default 1;
		DECLARE sTemp VARCHAR(4000) default '';
		
		IF length(trim(v_delimiter)) = 0  or v_delimiter is null then
		   SELECT '温馨提示：传入的分隔符为空或为NULL!' AS RESULT;
			 LEAVE label1;
		END IF;
		
		### 统计出分隔符在目标字符串中的次数
	  SET cnt_delimiter = (LENGTH(target_char) - LENGTH(REPLACE(target_char,v_delimiter,''))) / LENGTH(v_delimiter) ;
	
		IF cnt_delimiter = 0  then
			 SELECT '温馨提示：分隔符在目标字符串中不存在!请确认分隔符是否在目标字符串中。'  AS RESULT;
			 LEAVE label1;
		END IF;		

    DROP TEMPORARY table if exists t_delimiter;
		CREATE TEMPORARY TABLE t_delimiter(f_target varchar(200));
		
		 while i <= cnt_delimiter + 1 do
				INSERT INTO t_delimiter(f_target) VALUES(substring_index(substring_index(target_char,v_delimiter,i),v_delimiter,-1));	
				SET i = i + 1;	
		 end while;	
		SELECT * FROM  t_delimiter;
		
	  DROP TEMPORARY table if exists t_delimiter;	
END $$
DELIMITER ;