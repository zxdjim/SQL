DELIMITER $$
USE xyz$$
DROP PROCEDURE IF EXISTS sp_mvDB_from_old$$
CREATE PROCEDURE sp_mvDB_from_old(old_db varchar(50),new_db varchar(50))
BEGIN	
##  demo: call xyz.sp_mvDB_from_old('xyz_temp','xyz_temp_a');
    declare done int default false;
		declare v_table_schema varchar(50);
		declare v_table_name varchar(50);
		declare my_cur cursor for select table_schema,table_name from information_schema.tables where table_schema=old_db;
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;  
	  SET @SQL=CONCAT('create database if not exists  ',new_db,';');
	  PREPARE STMT FROM @SQL;
	  EXECUTE STMT;	
	  DEALLOCATE PREPARE STMT;
		open my_cur;
		read_loop:loop
		fetch my_cur into v_table_schema,v_table_name;
		IF done then
				leave read_loop;
		END IF;
		   SET @SQL=CONCAT('rename table ', v_table_schema,'.',v_table_name ,' to ',new_db,'.',v_table_name,';');
			 PREPARE STMT FROM @SQL;
			 EXECUTE STMT;	
		 	 DEALLOCATE PREPARE STMT;
		end loop;
		close my_cur;
END$$
DELIMITER ;

call xyz.sp_mvDB_from_old('xyz_temp','xyz_temp_a');

