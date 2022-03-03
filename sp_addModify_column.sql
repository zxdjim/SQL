use xyz;
DROP PROCEDURE IF EXISTS sp_addModify_column;
delimiter $$
CREATE PROCEDURE sp_addModify_column(in dbName varchar(50),in tableName varchar(50),in fieldName varchar(50),
    in fieldDataType varchar(50),in fieldDef varchar(50),in commentDef varchar(200),in afterField varchar(50))
begin
## DEMO: call sp_addModify_column('xyz','t_activity','ip','varchar(100)','NULL','comment \'ip地址\' ','after deviec_id');
## dbName:        库名  如 'xyz'
## tableName:     表名 如 't_activity'
## fieldName:     字段名 如 'ip'
## fieldDataType: 字段类型  如 'varchar(100)'
## fieldDef:      字段默认值 如 'NULL' 或 '0'
## commentDef:    字段注解  如 ' comment \'ip地址\' '(单引号用 \进行转义)
## afterField:    在某个字段后面 不写则放在最后 如 'after deviec_id'或 '',如果修改字段的话不写则不改变位置

IF NOT EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=dbName AND table_name=tableName AND COLUMN_NAME=fieldName) 
   AND EXISTS(SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=dbName AND table_name=tableName) THEN
		set @ddl=CONCAT('ALTER TABLE ',dbName,'.',tableName,' ADD COLUMN ',fieldName,' ',fieldDataType,'  default ',fieldDef,' ',commentDef, ' ',afterField);
		prepare stmt from @ddl;
		execute stmt;
		DEALLOCATE PREPARE stmt;
ELSEIF EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=dbName AND table_name=tableName AND COLUMN_NAME=fieldName) 
      AND EXISTS(SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=dbName AND table_name=tableName) THEN
		set @ddl=CONCAT('ALTER TABLE ',dbName,'.',tableName,' MODIFY ',fieldName,' ',fieldDataType,'  default ',fieldDef,' ',commentDef, ' ',afterField);
		prepare stmt from @ddl;
		execute stmt; 
        DEALLOCATE PREPARE stmt;		
END IF; 
end;
$$
delimiter ;

DROP PROCEDURE IF EXISTS sp_drop_column;
delimiter $$
CREATE PROCEDURE sp_drop_column(in dbName varchar(50),in tableName varchar(50),in fieldName varchar(50))
begin
## DEMO: call sp_drop_column('xyz','t_activity','ip');
## dbName:    库名  如 'xyz'
## tableName: 表名 如 't_activity'
## fieldName: 字段名 如 'ip'

IF EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=dbName AND table_name=tableName AND COLUMN_NAME=fieldName) 
   AND EXISTS(SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=dbName AND table_name=tableName) THEN
		set @ddl=CONCAT('ALTER TABLE ',dbName,'.',tableName,' DROP ',fieldName);
		prepare stmt from @ddl;
		execute stmt;
		DEALLOCATE PREPARE stmt;
END IF; 
end;
$$
