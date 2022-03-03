USE xyz_temp;
DELIMITER $$
drop procedure if exists sp_align_columns$$
create procedure sp_align_columns()
begin
### 此过程主要用于批量修改表的字段长度

    declare done int default false;	
	declare v_source_schema varchar(20);
	declare v_target_schema varchar(20);		
	declare v_table_name    varchar(50);

	
	declare my_cur cursor for select substring_index(source_table,'.',1), substring_index(target_table,'.',1), substring_index(target_table,'.',-1)       
							  from xyz.t_merge_db_init  where merge_status = 0;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;
	open my_cur;
	read_loop:loop
	fetch my_cur into v_source_schema, v_target_schema, v_table_name;
		IF done then
		   leave read_loop;
		END IF;
		
		call xyz.sp_addModify_column(v_source_schema,v_table_name,'CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
		call xyz.sp_addModify_column(v_target_schema,v_table_name,'CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
		call xyz.sp_addModify_column(v_source_schema,v_table_name,'LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
		call xyz.sp_addModify_column(v_target_schema,v_table_name,'LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
		IF LEFT(v_table_name,LENGTH(v_table_name)-6)='t_withdraw_order_' THEN 
			call xyz.sp_addModify_column(v_source_schema,v_table_name,'bank_branch','varchar(200)','NULL','comment "开户行地址" ','');
			call xyz.sp_addModify_column(v_target_schema,v_table_name,'bank_branch','varchar(200)','NULL','comment "开户行地址" ','');
		END IF;	
		
	end loop;
	close my_cur;
    SELECT '温馨提示:批量同步表的字段长度成功!' AS  RESULT;	
	
end $$
DELIMITER ;


### 同步源表和目标表中相关字段的长度
call sp_align_columns;

USE xyz_temp;
DELIMITER $$
drop procedure if exists sp_upd_repeated$$
create procedure sp_upd_repeated(v_schema varchar(50),v_table_name varchar(50),v_sum_field_by_key varchar(500),v_sum_field varchar(500),v_min_max bit)
label1:begin
### DEMO: call xyz.sp_upd_repeated('xyz','t_shareholder_report','BUNDLE_VERSION_ID,REPORT_DATE','MEMBER_COUNT,NEW_ANDROID_MEMBER_COUNT',0);
### v_schema        : 库名
### v_table_name    : 表名
### v_sum_field_by_key: 以逗号(英文)分隔的字符串形式的唯一字段(可依据此串创建唯一索引)
### v_sum_field       : 以逗号(英文)分隔的字符串形式的需要累加的字段串,如果此参数为NULL或空串,则不累加,直接删除重复且只保留最小/大记录
### v_min_max         : 需要更新和保留的最小/大的主键ID记录 0:min 1:max
### 功能说明: 此过程可用于检测某表中是否有重复记录,如果有,则依据传入的唯一字符串来累加更新,之后保留最小/大的记录
### author : jim   date: 2020-06-07

declare v_mm varchar(10) default 'min(';

if v_min_max = 0 then
   set v_mm ='min(';
else
   set v_mm ='max(';	
end if;

if v_sum_field_by_key is null or v_sum_field_by_key='' then
   select '温馨提示: 第三个传入参数不可为空,请重新输入后再重试!' as result;
	 leave label1;
end if;

set @v_schema_table = concat(v_schema,'.',v_table_name);
set @p_field = xyz.fn_get_table_fileds(v_schema,v_table_name,'P');
set @cnt = NULL;

#### 先查询一次是否有重复记录, 没有则提示后退出程序.
set @sql = concat('SELECT COUNT(*) into @cnt FROM ',@v_schema_table,' GROUP BY ',v_sum_field_by_key,' HAVING count(*) > 1;');					 
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

if @cnt IS NULL then
   select concat('温馨提示: 【',@v_schema_table,'】此表中依据(',v_sum_field_by_key,')分组后不存在重复记录!') as result;
	 leave label1;
end if;

#### 通过唯一字段来累加相关的字段数值, 如果v_sum_field为空,则不更新
IF v_sum_field IS NOT NULL AND TRIM(v_sum_field) != '' THEN 
		set @sql = concat('UPDATE ',@v_schema_table,' t inner join (SELECT ',v_mm,@p_field,') mm_id,',
							 v_sum_field_by_key,',',xyz.fn_StrContent_DDelimiter(',', 'sum(IFNULL(',',0)) ','', v_sum_field),' FROM ',@v_schema_table,
							 ' GROUP BY ',v_sum_field_by_key,' HAVING count(*) > 1) ta on t.',@p_field,'=ta.mm_id SET ',
							 xyz.fn_StrContent_DDelimiter(',', 't.',' = ta.','', v_sum_field),';');
        select @sql as result;							 
		PREPARE stmt FROM @sql;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
END IF;

### v_sum_field 累加字符串可为空;为空的话,只删除重复记录,不累加更新相关字段

#### 删除重复的记录,只保留最大/小的一条记录
set @new_condition = replace(v_sum_field_by_key, ',', ' and ');
set @sql = concat('DELETE t FROM ',@v_schema_table,' t inner join (SELECT ',v_mm,@p_field,') mm_id,', v_sum_field_by_key,' FROM ',@v_schema_table, ' GROUP BY ',
				   v_sum_field_by_key,' HAVING count(*) > 1) ta ON ',xyz.fn_StrContent_DDelimiter(' and ', 't.',' = ta.','', @new_condition),' and t.',@p_field,' != ta.mm_id;');
				   
select @sql as result;				   
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


end $$
DELIMITER ;
					
DELIMITER $$
DROP PROCEDURE IF EXISTS sp_merge_db$$
CREATE PROCEDURE sp_merge_db(V_KeyValue_Add int)
label1:BEGIN
### call sp_merge_db(1000000);
### V_KeyValue_Add     用户ID要加的数值
### 【重要说明】: 此过程依赖 xyz.t_merge_db_init 和 xyz.t_merge_db_log这2张表,请调用此过程前保证2张表正确创建及插入正确记录.
### author : jim   date: 2019-11-18
### modify by 2020-01-30 删除对目标表自增长修改(会自动增长)和加后缀的参数(前面步骤处理)

set @ti_cnt = 0;
select IFNULL(count(*),0) into @ti_cnt from xyz.t_merge_db_init where source_table in ('03.sql','04.sql','05.sql') and merge_status = 1;
IF @ti_cnt < 3 then
  SELECT '温馨提示：上一步没有操作成功,请先完成上一步操作!' AS result;
  LEAVE label1;
END IF;
			
IF V_KeyValue_Add < 10000 or V_KeyValue_Add is null  then
  SELECT  '传入的第一个参数,必须大于10000,否则可能会产生主键冲突问题!' AS result;
  LEAVE label1;
END IF;	
 
	### 设置 group_concat的最大值,原值为:1024过小.
  SET group_concat_max_len=102400;
	set session sql_mode='';
  BEGIN
	    declare v_table_cnt      int default 0;           ## 源表和目标表数量
			declare v_update_cnt     int default 0;           ## 要更新的字段数量
			declare v_pk_cnt         int default 0;           ## 特殊列的数量
			declare i                int default 1;           ## 字段列中字段个数,做循环处理
	    declare done             int default false;       ## 游标开关
			declare v_batch_id       int default 1;           ## 批次ID
			declare sTemp_field      varchar(500) default ''; ## 要更新的字段列中,单个字段值
	    declare v_source_cnt     int default 0;           ## 源表总记录数
			declare v_target_cnt     int default 0;           ## 目标表总记录数
			declare v_target_sum_cnt int default 0;           ## 目标表预估总记录数
			
			### 游标中变量
			declare v_source_table      varchar(200);        ## 源表 source_schema.table_name 格式
		  declare v_target_table      varchar(200);        ## 目标表 target_schema.table_name
			declare v_force_check       bit default 1;       ## 是否进行强类型一致性检查, 0-否 1-是			
		  declare v_init_type         tinyint;             ## 0-只需复制到新库; 1-关键ID需要加某个数值(比如100000)等操作,然后再复制到新库 
		  declare v_sum_type          bit default 0;       ## 0-不需要累加; 1-需要累加(没找到则新增,找到则更新累加)
		  declare v_sum_field         varchar(8000);       ## 需要累加的字段名(以英文半角逗号分隔),没有则为NULL
			declare v_sum_field_temp    varchar(8000) default ','; ## 需要累加的字段名(以英文半角逗号分隔),没有则为NULL
		  declare v_sum_field_by_key  varchar(500);        ## 需要累加所依赖的唯一键(以英文半角逗号分隔,以改为调用过程创建唯一索引),没有则为NULL
			
      ### 源类变量(与目标类变量相对应)			
			declare v_sPField                varchar(50);    ##源主键
			declare v_sAField                varchar(50);    ##源自增键
			declare v_sFields_list           varchar(2000);  ##源最终列表(处理后的,要跟目标最终列表一致)
		  declare v_sNAFields_list         varchar(2000);  ##源非自增键列表
		  declare v_sAFields_list          varchar(2000);  ##源含自增键列表
			declare v_sAFields_type_list     varchar(4000);  ##源含自增键且带数据类型列表 
			
			### 目标类变量(与源类变量相对应)
		  declare v_tPField                varchar(50);    ##目标主键
			declare v_tAField                varchar(50);    ##目标自增键			
			declare v_tFields_list           varchar(2000);  ##目标最终列表(处理后的,要跟源最终列表一致)
		  declare v_tNAFields_list         varchar(2000);  ##目标非自增键列表
		  declare v_tAFields_list          varchar(2000);  ##目标含自增键列表
			declare v_tAFields_type_list     varchar(4000);  ##目标含自增键且带数据类型列表 
			declare v_sql_ok                 bit default 0;  ##v_sql是否正常执行,默认为0:不正常,1:正常         
			
			declare my_cur cursor for select source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key from xyz.t_merge_db_init 
		                          	where merge_status = 0 and is_batch = 0 order by source_table desc;
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;
		  select if(max(batch_id) is null,1,max(batch_id) + 1) into v_batch_id from xyz.t_merge_db_log;	
			insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,'START merge db...');
			open my_cur;
			read_loop:loop
			fetch my_cur into v_source_table,v_target_table,v_force_check,v_init_type,v_sum_type,v_sum_field,v_sum_field_by_key;
					IF done then
						leave read_loop;
					END IF;
					
					### 一开始赋值为0
					set v_sql_ok = 0;
					### 开始对每一行进行处理
					insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status)
					values(v_batch_id,CONCAT('    开始:源表【',v_source_table,'】和 目标表【',v_target_table,'】...'),v_source_table,0);
					select CONCAT('开始:源表【',v_source_table,'】和 目标表【',v_target_table,'】...') as result;
							
					select count(*) into v_table_cnt from information_schema.tables  where concat(TABLE_SCHEMA,'.',table_name) in (v_source_table,v_target_table);
					IF v_table_cnt < 2 THEN
							insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status)
							values(v_batch_id,CONCAT('        ERROR:源表【',v_source_table,'】和 目标表【',v_target_table,'】中至少一个不存在 !!!'),v_source_table,0);
							select CONCAT('ERROR:源表【',v_source_table,'】和 目标表【',v_target_table,'】中至少一个不存在 !!!') as result;
							ITERATE read_loop;							   
					END IF;
			
			    SET @SQL = '';
	        #### 源的字段列表
					select UPPER(GROUP_CONCAT(if(column_key='PRI',COLUMN_NAME,null) order by ORDINAL_POSITION)) sPField,
					       UPPER(if(extra='auto_increment',COLUMN_NAME,null)) sAFields_list,					
					       UPPER(GROUP_CONCAT(if(extra='auto_increment',null,COLUMN_NAME) order by ORDINAL_POSITION)) sNAFields_list,
					       UPPER(GROUP_CONCAT(COLUMN_NAME order by ORDINAL_POSITION)) sAFields_list,
								 UPPER(GROUP_CONCAT(if(upper(data_type) in ('TINYINT','SMALLINT','MEDIUMINT','INT','BIGINT'),CONCAT(COLUMN_NAME,' ',data_type),
								       CONCAT(COLUMN_NAME,' ',column_type))  order by ORDINAL_POSITION)) sAFields_type_list
								 into v_sPField,v_sAField,v_sNAFields_list,v_sAFields_list,v_sAFields_type_list 
					  from information_schema.`COLUMNS` 
					  where concat(TABLE_SCHEMA,'.',table_name) = v_source_table;
					
					#### 目标的字段列表	'TINYINT','SMALLINT','MEDIUMINT','INT','BIGINT'中带不带(X) 不影响数据类型的精度,所以此处去掉后缀,其他类型则需要.(demo:int(5)->此处简化为int)
					select UPPER(GROUP_CONCAT(if(column_key='PRI',COLUMN_NAME,null)  order by ORDINAL_POSITION)) tPField,
					       UPPER(if(extra='auto_increment',COLUMN_NAME,null)) tAFields_list,								
					       UPPER(GROUP_CONCAT(if(extra='auto_increment',null,COLUMN_NAME)  order by ORDINAL_POSITION)) tNAFields_list,
								 UPPER(GROUP_CONCAT(COLUMN_NAME order by ORDINAL_POSITION)) tAFields_list,
					       UPPER(GROUP_CONCAT(if(upper(data_type) in ('TINYINT','SMALLINT','MEDIUMINT','INT','BIGINT'),CONCAT(COLUMN_NAME,' ',data_type),
								       CONCAT(COLUMN_NAME,' ',column_type))  order by ORDINAL_POSITION)) tAFields_type_list
				         into v_tPField,v_tAField,v_tNAFields_list,v_tAFields_list,v_tAFields_type_list 
								 from information_schema.`COLUMNS` 
		      where concat(TABLE_SCHEMA,'.',table_name) = v_target_table;
				  
					### 判断结构是否一致，否则跳过本次循环 
					### 一 先对主键进行检查
				  IF  ifnull(v_sPField,'') = ifnull(v_tPField,'') and  ifnull(v_sAField,'') = ifnull(v_tAField,'') 	THEN  
					    ### 二 再对列名(只针对名称,不带类型)进行检查,此处考虑目标表中字段顺序可能和源表不一致,
							###  所以放弃使用 sNPFields_list和tNPFields_list 相等性检查;
							###  另外,如果目标表比源表多了字段,也必须使用locate函数进行查找,这样更灵活.
							
							### -- xyz.fn_get_exists 和 直接对比 两者取一种方法
						  ### -- 1 从源表取字段到目标表中查找,找不到则说明列名不一致,此处进行严格检查
						  	if xyz.fn_get_exists(',',v_sNAFields_list,',',v_tNAFields_list) = 0 then
						  			insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status,source_field_error,target_field_error)
						  			values(v_batch_id,CONCAT('        ERROR:源表【',v_source_table,'】中有字段 在目标表【',v_target_table,'】中不存在 !!!'),
						  						 v_source_table,0,v_sNAFields_list,v_tNAFields_list);
						  			select CONCAT('ERROR:源表【',v_source_table,'】中有字段 在目标表【',v_target_table,'】中不存在 !!!') as result;
						  			ITERATE read_loop;										   
						  	end if;		
							
							### 2 此处也可以进行 弱检查 v_sNAFields_list != v_tNAFields_list							
							### if v_sNAFields_list != v_tNAFields_list then
							### 		insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status,source_field_error,target_field_error)
							### 		values(v_batch_id, '        ERROR:【源表和目标表 字段不一致】 !!!',
							### 					 v_source_table,0,v_sNAFields_list,v_tNAFields_list);
							### 		select 'ERROR:【源表和目标表中 字段不一致】 !!!'  as result;
							### 		ITERATE read_loop;										   
							### end if;	
							
							###  正常检查通过,打印日志和输出控制台
							insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status)
							values(v_batch_id, '        字段检查通过:源表和目标表 字段一致 !!!',v_source_table,0);
							select '字段检查通过:源表和目标表 字段一致 !!!' as result;
											
							### 三 最后由表字段中force_check(0或1)来区分是否进行强类型一致性检查
							IF V_force_check = 1 THEN 
							### 从源表取字段+类型到目标表中查找,找不到则说明列名不一致
							 		if xyz.fn_get_exists(',',v_sAFields_type_list,',',v_tAFields_type_list) = 0 then
							 				### 从源表取字段到目标表中查找,找不到则说明列名不一致
							 				insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status,source_field_error,target_field_error)
							 				values(v_batch_id, '        ERROR:【源表和目标表 数据类型不一致】 !!!',
							 							 v_source_table,0,v_sAFields_type_list,v_tAFields_type_list);
							 				select  'ERROR:【源表和目标表 数据类型不一致】 !!!' as result;
							 				ITERATE read_loop;
                  end if;	
									
									### -- xyz.fn_get_exists 和 直接对比法 两者取一种方法 同上
								###	if v_sAFields_type_list != v_tAFields_type_list then
								###			### 从源表取字段到目标表中查找,找不到则说明列名不一致
								###			insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status,source_field_error,target_field_error)
								###			values(v_batch_id, '        ERROR:【源表和目标表 数据类型不一致】 !!!',
								###						 v_source_table,0,v_sAFields_type_list,v_tAFields_type_list);
								###			select  'ERROR:【源表和目标表 数据类型不一致】 !!!' as result;
								###			ITERATE read_loop;
                ### end if;										

									insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status)
									values(v_batch_id, '        类型检查通过:源表和目标表 数据类型一致 !!!',v_source_table,0);
									select '类型检查通过:源表和目标表 数据类型一致 !!!' as result;
							ELSE
									insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status,source_field_error,target_field_error)
									values(v_batch_id, '        温馨提示:未进行强数据类型一致性检查,可能会存在丢失数据风险,默认为继续 !!!',
												 v_source_table,0,v_sAFields_type_list,v_tAFields_type_list);
									select '温馨提示:未进行强数据类型一致性检查,可能会存在丢失数据风险,默认为继续  !!!' as result;		
							END IF;		
					ELSE 
						  insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status,source_field_error,target_field_error)
				      values(v_batch_id,'        ERROR:源表和目标表 中主键或自增键不一致 !!!',v_source_table,0,v_sAFields_list,v_tAFields_list);
							select 'ERROR:源表和目标表 中主键或自增键不一致 !!!' as result;
					    ITERATE read_loop;					
					END IF;
					
					### 判断特殊列是否主键,是主键则插入时需必须带主键,否则不需带主键  
					select count(1) into v_pk_cnt from dual where v_sPField = v_sAField  and 
					        v_sPField in ('MEMBER_ID','USER_ID','PROXY_LINK_CODE','ORDER_CODE','RECHARGE_ORDER_CODE','TASK_ID',
					                      'PARENT_ID','MEMBER_ACCOUNT','USER_ACCOUNT','USER_SYSTEM_ID','PLATFROM_ORDER_CODE');
															
					IF v_pk_cnt >= 1 THEN ## 带自增键列表
						 SET v_sFields_list = v_sAFields_list;
						 SET v_tFields_list = v_tAFields_list;
					ELSE  ## 不带自增键列表
					   SET v_sFields_list = v_sNAFields_list;
						 SET v_tFields_list = v_tNAFields_list;
					END IF;
					
					### 到此处时 【主键】+【字段】+【数据类型】都检查完成,说明源和目标结构一致.
					### 可以把源字段列表赋值给目标列表,以防止目标列表比源多出字段且顺序不一问题
					### 赋值完成之后,就可以对v_sFields_list源字段列表进行特殊处理了.
					SET v_tFields_list = v_sFields_list;

					### 前后加 , 方便后面对字段进行处理,最后再去掉前后的逗号
					SET v_sFields_list = CONCAT(',',v_sFields_list,',');
					
					### 对特殊列的加以处理(4种)
					### 1 proxy_link_code
					### 2 MEMBER_ID USER_ID PARENT_ID TASK_ID     
					### 3 MEMBER_ACCOUNT USER_ACCOUNT USER_SYSTEM_ID(有重复的加后缀,否则保持原样)
					### 4 ORDER_CODE,RECHARGE_ORDER_CODE,PLATFROM_ORDER_CODE(DEMO:R4-6155520191008233300762 其中 61555 为用户ID)
					
					IF v_init_type = 1 THEN
							### 1 proxy_link_code  xyz.fn_NumContent_delimiter()  把第二个参数和后面的子串中的数字相加
							SET v_sFields_list = REPLACE(v_sFields_list,',PROXY_LINK_CODE,',concat(',concat(xyz.fn_NumContent_delimiter(\'_\',',V_KeyValue_Add,',PROXY_LINK_CODE)),')); 

							### 2 MEMBER_ID USER_ID PARENT_ID TASK_ID   
							SET v_sFields_list = REPLACE(v_sFields_list,',USER_ID,',concat(',USER_ID + ',V_KeyValue_Add,','));
							SET v_sFields_list = REPLACE(v_sFields_list,',MEMBER_ID,',concat(',MEMBER_ID + ',V_KeyValue_Add,','));
							SET v_sFields_list = REPLACE(v_sFields_list,',PARENT_ID,',concat(',PARENT_ID + ',V_KeyValue_Add,','));
                            SET v_sFields_list = REPLACE(v_sFields_list,',TASK_ID,',concat(',TASK_ID + ',V_KeyValue_Add,','));
							
							### 3 MEMBER_ACCOUNT USER_ACCOUNT USER_SYSTEM_ID (提前在sp_update_source_db过程中处理了)
							## SET v_sFields_list = IF(xyz.fn_get_exists(',','MEMBER_ACCOUNT',',',v_sFields_list) = 1,
							##     REPLACE(v_sFields_list,'MEMBER_ACCOUNT',concat('concat(MEMBER_ACCOUNT,\'',V_Account_postfix,'\')')),v_sFields_list);
							## SET v_sFields_list = IF(xyz.fn_get_exists(',','USER_ACCOUNT',',',v_sFields_list) = 1,  
							##     REPLACE(v_sFields_list,'USER_ACCOUNT',concat('concat(USER_ACCOUNT,\'',V_Account_postfix,'\')')),v_sFields_list);
							## SET v_sFields_list = IF(xyz.fn_get_exists(',','USER_SYSTEM_ID',',',v_sFields_list) = 1,
							##     REPLACE(v_sFields_list,'USER_SYSTEM_ID',concat('concat(USER_SYSTEM_ID,\'',V_Account_postfix,'\')')),v_sFields_list);
		
							### 4 ORDER_CODE,RECHARGE_ORDER_CODE,PLATFROM_ORDER_CODE
							SET v_sFields_list =
									REPLACE(v_sFields_list,',ORDER_CODE,',concat(',REPLACE(ORDER_CODE,CONCAT(\'-\',USER_ID),',concat('concat(\'-\',','USER_ID + ',V_KeyValue_Add,')'),'),'));
							SET v_sFields_list = 
									REPLACE(v_sFields_list,',RECHARGE_ORDER_CODE,',concat(',REPLACE(RECHARGE_ORDER_CODE,CONCAT(\'-\',USER_ID),',concat('concat(\'-\',','USER_ID + ',V_KeyValue_Add,')'),'),'));	
							SET v_sFields_list = 
									REPLACE(v_sFields_list,',PLATFROM_ORDER_CODE,',concat(',REPLACE(PLATFROM_ORDER_CODE,CONCAT(\'-\',USER_ID),',concat('concat(\'-\',','USER_ID + ',V_KeyValue_Add,')'),'),'));	
		
					END IF;
					### 去掉前面的逗号,如果有的话
					IF LEFT(v_sFields_list,1) = ',' THEN
					   SET v_sFields_list = RIGHT(v_sFields_list,LENGTH(v_sFields_list) - 1);
					END IF;
					IF RIGHT(v_sFields_list,1) = ',' THEN
					   SET v_sFields_list = LEFT(v_sFields_list,LENGTH(v_sFields_list) - 1);
					END IF;				
					
				  SET @SQL ='';
				  ## INSERT INTO table (a,b,c) SELECT A,B,C FROM XXX ON DUPLICATE KEY UPDATE field1 = field1 + values(field1);
					IF  v_sum_type = 0 THEN  ## 只需复制到新库且不需累加 + 关键字段特殊处理,然后再复制到新库且不需要累加
						 SET @SQL = concat('insert into ',v_target_table,'(',v_tFields_list,') select ',v_sFields_list,'  from ',v_source_table,';');
					ELSEIF v_init_type = 0 AND v_sum_type = 1 THEN	## 只需复制到新库且需累加
					
					    ### sum_field 不可为空
					    IF v_sum_field is null or length(trim(v_sum_field)) = 0 then
							   	insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status,source_field_error,target_field_error)
								   	values(v_batch_id,CONCAT('        ERROR:t_merge_db_init表中source_table=\'',v_source_table,'\' 此行,字段sum_field 为NULL或空串!!!'),
										       v_source_table,0,v_sAFields_list,v_tAFields_list);
									select CONCAT('        ERROR:t_merge_db_init表中source_table=\'',v_source_table,'\' 此行,字段sum_field 为NULL或空串!!!') as result;
									ITERATE read_loop;	
							ELSE  
							    ## replace field1 = field1 + values(field1)  DEMO :v_sum_field =',MEMBER_COUNT,NEW_ANDROID_MEMBER_COUNT,'
									SET v_sum_field = TRIM(v_sum_field);
									IF LEFT(v_sum_field,1) !=',' THEN
									   SET v_sum_field = CONCAT(',',v_sum_field);
									END IF;
									IF RIGHT(v_sum_field,1) !=',' THEN
									   SET v_sum_field = CONCAT(v_sum_field,',');
									END IF;
									
					     		set v_update_cnt = LENGTH(v_sum_field) - LENGTH(REPLACE(v_sum_field,',',''));
									set v_sum_field_temp = ',';
									set i = 1;
									while i <= v_update_cnt do
									    set sTemp_field = trim(substring_index(substring_index(v_sum_field,',',i),',',-1));
											if sTemp_field != '' and sTemp_field is not null then
                         set v_sum_field = replace(v_sum_field,CONCAT(',',sTemp_field,','),concat(',',v_target_table,'.',sTemp_field,'=',
												                           v_target_table,'.',sTemp_field,'+VALUES(',sTemp_field,')',','));	 
											end if;	
											SET i = i + 1;
									end while;
									SET v_sum_field = right(left(v_sum_field,length(v_sum_field)-1),length(v_sum_field)-2);
							END IF;
							
							### sum_field_by_key 不可为空
					    SET @SQL ='';
					    IF v_sum_field_by_key IS NULL OR length(trim(v_sum_field_by_key)) = 0 then
							   insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status,source_field_error)
									 values(v_batch_id,CONCAT('        ERROR:批次为',v_batch_id,' 源表为',v_target_table,' sum_field_by_key 为NULL或空串!!!'),
									        v_source_table,0,v_sum_field_by_key);
							   SELECT concat('ERROR:批次为',v_batch_id,' 源表为',v_target_table,' sum_field_by_key 为NULL或空串!!!') AS result;
								 ITERATE read_loop;	 
					    ELSE 
									### 插入唯一索引前后记录此操作 call xyz.sp_create_index('xyz','t_merge_db_log',1,'idx_t_merge_db_log_id','batch_id');
									insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status) values(v_batch_id,CONCAT('        开始创建唯一索引(如果目标表中有重复数据则先行处理)... '),v_source_table,0);
									SELECT '开始创建唯一索引(如果目标表中有重复数据则先行处理)... ' as Result;
                      ### 创建唯一索引前,处理目标重复数据 									
					  call xyz_temp.sp_upd_repeated(left(v_target_table,locate('.',v_target_table) - 1),right(v_target_table,length(v_target_table) - locate('.',v_target_table)),v_sum_field_by_key,v_sum_field,0);
					  
		              call xyz.sp_create_index(left(v_target_table,locate('.',v_target_table) - 1),
									      right(v_target_table,length(v_target_table) - locate('.',v_target_table)), 0, 
												concat('uidx_', right(v_target_table,length(v_target_table) - locate('.',v_target_table)),'_01'),v_sum_field_by_key);
												
									insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status) values(v_batch_id,CONCAT('        结束创建唯一索引!!! '),v_source_table,0);	
	                SELECT '结束创建唯一索引!!! ' as Result;								
							END IF;	 
		
					    SET @SQL ='';
					    SET @SQL = concat('insert into ',v_target_table,'(',v_tFields_list,') select ',v_sFields_list,'  from ',v_source_table,' ON DUPLICATE KEY UPDATE ',v_sum_field,';');
												
					END IF;
					
					IF @SQL != '' and @SQL is not null THEN
					  	insert into xyz.t_merge_db_log(batch_id,memo,sql_memo,source_table,batch_status) 
							                    values(v_batch_id,CONCAT('        开始对目标表【',v_target_table,'】插入数据... '),@SQL,v_source_table,0);
						  SELECT concat('开始对目标表【',v_target_table,'】插入数据... ') as Result;
							
							### 插入数据前,统计源和目标表的记录数
							set @v_source_cnt = 0;
							set @v_target_cnt = 0;
							set @v_target_sum_cnt = 0;
							SET @SQL_QTY  = '';
							SET @SQL_QTY = concat('select count(*) into @v_source_cnt from ',v_source_table,';');
							PREPARE STMT FROM @SQL_QTY;
							EXECUTE STMT;	
							DEALLOCATE PREPARE STMT;	
							
							SET @SQL_QTY  = '';
							SET @SQL_QTY = concat('select count(*) into @v_target_cnt from ',v_target_table,';');
							PREPARE STMT FROM @SQL_QTY;
							EXECUTE STMT;	
							DEALLOCATE PREPARE STMT;	
							
							SET @v_target_sum_cnt = @v_source_cnt + @v_target_cnt;
					  	insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status) 
							                    values(v_batch_id,CONCAT('        插入数据前数量统计:源表【',@v_source_cnt,'】,目标表【',@v_target_cnt,'】 预估量=源表量+目标表量'),v_source_table,0);
						  SELECT CONCAT('插入数据前数量统计:源表【',@v_source_cnt,'】,目标表【',@v_target_cnt,'】 预估量=源表量+目标表量') as Result;
							
							PREPARE STMT FROM @SQL;
							EXECUTE STMT;	
							DEALLOCATE PREPARE STMT;															
							
							### 插入数据后,统计目标表的记录数
							set @v_target_cnt = 0;
							SET @SQL_QTY  = '';
							SET @SQL_QTY = concat('select count(*) into @v_target_cnt from ',v_target_table,';');
							PREPARE STMT FROM @SQL_QTY;
							EXECUTE STMT;	
							DEALLOCATE PREPARE STMT;	
							
					  	insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status) 
							                    values(v_batch_id,CONCAT('        插入数据后数量统计(目标表): 实际量【',@v_target_cnt,'】 预估量【',@v_target_sum_cnt,'】 实际量>=预估量 才是正常情况!!!'),v_source_table,1);
						  SELECT CONCAT('插入数据后数量统计(目标表): 实际量【',@v_target_cnt,'】 预估量【',@v_target_sum_cnt,'】 实际量>=预估量 才是正常情况!!!') as Result;		

						  insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status) values(v_batch_id,CONCAT('        结束对目标表【',v_target_table,'】插入数据!!! '),v_source_table,1);
						  SELECT concat('结束对目标表【',v_target_table,'】插入数据!!! ') as Result;	
                          #### 赋值为1,V_SQL正常执行完成
						  set v_sql_ok = 1;
          END IF;
		
					### 结束对本行进行处理
					insert into xyz.t_merge_db_log(batch_id,memo,source_table,batch_status)
					values(v_batch_id,CONCAT('    结束:源表【',v_source_table,'】和 目标表【',v_target_table,'】!!!'),v_source_table,0);
					select CONCAT('结束:源表【',v_source_table,'】和 目标表【',v_target_table,'】!!!') as result;
					
					IF v_sql_ok = 1 then
						### 完成之后对此记录的 merge_status 置为 1为成功,后续不再操作此记录,同时合并日志batch_status 状态置为1
						update xyz.t_merge_db_init set merge_status = 1 where source_table = v_source_table;
						update xyz.t_merge_db_log  set batch_status = 1 where batch_id = v_batch_id and source_table = v_source_table;
					END IF;	
			end loop;
			close my_cur;						
			
            SELECT COUNT(*) INTO @cnt FROM xyz.t_merge_db_init WHERE merge_status = 0;
            IF @cnt = 0 then
			   insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,'FINISH merge db!!!');
               update xyz.t_merge_db_init set merge_status = 1, last_updated_time = now() where source_table='06.sql';			   
			   SELECT '成功提示:恭喜已完成【一次性】合并数据全部步骤!' as result;			   
			ELSE
			   insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,CONCAT('温馨提示:您还有【',@cnt,'】张表没有完成合并,请处理后再重新执行本步骤!'));			
			   SELECT CONCAT('温馨提示:您还有【',@cnt,'】张表没有完成合并,请处理后再重新执行本步骤!') as ERROR;	
			   SELECT '****************以下为打印合并异常日志开始...***************' as ERROR;
			   select memo,sql_memo,source_table,source_field_error,target_field_error from xyz.t_merge_db_log where source_table in 
			         (select  source_table from  xyz.t_merge_db_init where merge_status = 0) and batch_id = v_batch_id;
			   SELECT '****************以上为打印合并异常日志结束!!!***************' as ERROR;
			END IF;
  END;
END$$
DELIMITER ;

###  这里视实际情况来决定传递什么参数(由要合并入的会员表中最大的会员ID值取得).
select max(KeyValue_Add) into @add_max from xyz.t_merge_db_init;
call sp_merge_db(@add_max);
