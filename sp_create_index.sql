DELIMITER $$
#该表所在数据库名称
USE xyz$$
DROP PROCEDURE IF EXISTS sp_create_index$$
CREATE PROCEDURE sp_create_index(V_index_schema varchar(50),v_tableName varchar(50),v_is_unique bit,v_index_name varchar(50),v_index_list varchar(2000))
label1:BEGIN	
## DEMO: call sp_create_index('xyz','t_member_user',1,'idx_t_member_user_name_id','user_name,user_id');
## V_index_schema:  库名         如 'xyz'
## v_tableName:     表名称       如 't_member_user'
## v_is_unique:     是否唯一索引 如 0:是  1:否
## v_index_name:    索引名称     如 'idx_t_member_user_name_id'(idx:一般索引,uidx:唯一索引 +表名+列名缩写 中间用_分隔)
## v_index_list:    索引内容     如 'user_name,user_id' (复合索引时,中间用英文半角逗号分隔','不要用括号和多余空格)

  DECLARE ROWS_CNT tinyint default 0;	
	### not exists table is exit;
	SELECT count(1) into ROWS_CNT FROM INFORMATION_SCHEMA.tables where upper(table_schema)=upper(V_index_schema)  and  upper(table_name) = upper(v_tableName);
	IF  ROWS_CNT = 0 THEN
			SELECT CONCAT('ERROR: 表 【',v_tableName,'】不存在!!!') as result;
			leave label1;
	END IF;
	
	IF  locate('(',v_index_list) > 0 or locate(')',v_index_list) > 0 THEN
			SELECT CONCAT('ERROR: 索引内容 :【',v_index_list,'】 中含有括号字符，请确认取消后再试！') as result;
			leave label1;
	END IF;
	
 begin
    declare done int default false;	
	  declare V_index_list_old varchar(2000);
		
	  ## inner cursor variables;
	  declare V_index_name_old varchar(50);
    ####  index is or not same 0:not same  1:same
	  declare V_is_same bit default 0;
	
		DECLARE my_cur CURSOR for SELECT distinct index_name  FROM INFORMATION_SCHEMA.STATISTICS WHERE upper(index_schema) = upper(V_index_schema) 
					            	and upper(table_name) = upper(v_tableName)  and  non_unique = v_is_unique  ORDER BY index_name;
	   DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;   
	
		OPEN my_cur;
		read_loop:loop
	    FETCH my_cur INTO  V_index_name_old;			
			### if end of record is leave
			IF done then
				leave read_loop;
			END IF;
				
			SELECT GROUP_CONCAT(column_name order by Seq_in_index) into V_index_list_old 
			FROM INFORMATION_SCHEMA.STATISTICS WHERE upper(index_schema) = upper(V_index_schema) and upper(table_name) = upper(v_tableName)  
						and  non_unique = v_is_unique  and index_name = V_index_name_old;
	
			IF upper(trim(v_index_list)) = upper(V_index_list_old) then
				 set V_is_same = 1;
				 leave read_loop;
			END IF;
		end loop;	
	  close my_cur;
					
			SET @SQL = '';
			IF V_is_same = 0 then
				SET ROWS_CNT = 0;			
				SELECT count(1) into ROWS_CNT FROM INFORMATION_SCHEMA.STATISTICS where index_schema=V_index_schema AND table_name=v_tableName 
			      AND index_name=v_index_name AND index_name !='PRIMARY';
			    ## but have same index_name
					IF ROWS_CNT > 0 then
						 SET v_index_name = CONCAT(v_index_name,'_01');
					END IF;
	
					## IS OR NOT UNIQUE INDEX
					IF v_is_unique = 0 THEN 
					 SET @SQL = CONCAT('create unique index ', v_index_name, ' on ',V_index_schema,'.',v_tableName, '(',v_index_list,');'); 
					ELSEIF v_is_unique = 1 THEN 
						SET @SQL = CONCAT('create index ', v_index_name, ' on ',V_index_schema,'.',v_tableName, '(',v_index_list,');');
					END IF;			
			ELSE 
				SELECT CONCAT('表 :【',v_tableName,'】中索引 :【',v_index_list,'】已存在,请勿重复创建!!!') as result; 					   
			END IF;
			
			IF @SQL!='' THEN			
					PREPARE STMT FROM @SQL;
					EXECUTE STMT;	
					DEALLOCATE PREPARE STMT;		
			END IF;			
   END;
	 
END$$
DELIMITER ;