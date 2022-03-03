USE xyz_temp;
DELIMITER $$
drop procedure if exists sp_update_source_db$$
create procedure sp_update_source_db()
label1:begin
    #### 先检测上一步是否操作成功
	set @ti_cnt = 0;
	select IFNULL(count(*),0) into @ti_cnt from xyz.t_merge_db_init where source_table in ('03.sql','04.sql') and merge_status = 1;
	IF @ti_cnt < 2 then
	  SELECT '温馨提示：上一步没有操作成功,请先完成上一步操作!' AS result;
	  LEAVE label1;
	END IF;	
begin
### 此过程主要用于更新源表中 临时库的 MEMBER_ACCOUNT和USER_ACCOUNT 重复账号加后缀问题

      declare done int default false;	
      declare rec_cnt int default 0;	  
			declare v_batch_id int default 0;	
      declare v_field_name varchar(50);
			declare v_source_table varchar(100);			
			declare v_update_sql varchar(1000);
			
			declare my_cur    cursor for select 'MEMBER_ACCOUNT' field_name  union select 'USER_ACCOUNT';
			declare my_cur_up cursor for select source_table,concat('update ',source_table,' ts inner join  xyz_temp.tmp_member_user tmp on tmp.old_user_account=ts.',
					    v_field_name,' set ts.',v_field_name,' = tmp.new_user_account;') 
						  update_sql  from xyz.t_merge_db_init t where merge_status = 0 and exists 
					 (select 1 from information_schema.`COLUMNS` 
					 where t.source_table = concat(table_schema,'.',table_name) and column_name=v_field_name) and source_table!='xyz_temp.t_member_user';
					 
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;
		  select if(max(batch_id) is null,1,max(batch_id) + 1) into v_batch_id from xyz.t_merge_db_log;	
			insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,'START update source db...');
			SELECT 'START update source db...'  AS result;
			open my_cur;
			read_loop:loop
			fetch my_cur into v_field_name;
				IF done then
					leave read_loop;
				END IF;
				
					open my_cur_up;
					read_loop_up:loop
					fetch my_cur_up into v_source_table,v_update_sql;
							IF done then
								leave read_loop_up;
							END IF;
							SET @SQL = '';
							SET @SQL = v_update_sql;
							insert into xyz.t_merge_db_log(batch_id,memo,sql_memo,source_table) 
							  values(v_batch_id,concat('Begin update source table 【',v_source_table,'】...'),@SQL,v_source_table);
							SELECT concat('Begin update source table 【',v_source_table,'】...') AS result;
							PREPARE STMT FROM @SQL;
							EXECUTE STMT;	
							DEALLOCATE PREPARE STMT;	
							insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,concat('End update source table 【',v_source_table,'】!!!'));
							SELECT concat('End update source table 【',v_source_table,'】!!!') AS result;
					end loop;
			    close my_cur_up;
					set done = false;
			end loop;
			close my_cur;
			
			insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,'Finish update source db!!!');
			SELECT 'Finish update source db...'  AS result;
            SELECT COUNT(*) INTO rec_cnt FROM xyz.t_merge_db_log WHERE batch_id = v_batch_id;					
			IF rec_cnt > 2 and mod(rec_cnt, 2) = 0 then
			   SELECT '成功提示:源表中相关字段处理完成,请进行【下一步】操作!' as result;
			   update xyz.t_merge_db_init set merge_status = 1, last_updated_time = now()  where source_table='05.sql';
			ELSE
			   SELECT '警告提示:源表中没有字段需要处理,请确认是否正常!' as result;
			END IF;	
end;			
end $$
DELIMITER ;

call sp_update_source_db;

### 对源库中5张相关表修改创建人和修改人长度为 varchar(30)
call xyz.sp_addModify_column('xyz_temp','t_proxy_wallet','CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
call xyz.sp_addModify_column('xyz_temp','t_proxy_wallet','LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
call xyz.sp_addModify_column('xyz_temp','t_receipt_offline_financial_report','CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
call xyz.sp_addModify_column('xyz_temp','t_receipt_offline_financial_report','LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
call xyz.sp_addModify_column('xyz_temp','t_member_agency_settlement','CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
call xyz.sp_addModify_column('xyz_temp','t_member_agency_settlement','LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
call xyz.sp_addModify_column('xyz_temp','t_discount_financial_report','CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
call xyz.sp_addModify_column('xyz_temp','t_discount_financial_report','LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
call xyz.sp_addModify_column('xyz_temp','t_commission_settlement_record','MEMBER_ACCOUNT','varchar(30)','NULL','comment \'用户账号\' ','after MEMBER_ID');

### 对目标库中5张相关表修改创建人和修改人长度为 varchar(30)
call xyz.sp_addModify_column('xyz','t_proxy_wallet','CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
call xyz.sp_addModify_column('xyz','t_proxy_wallet','LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
call xyz.sp_addModify_column('xyz','t_receipt_offline_financial_report','CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
call xyz.sp_addModify_column('xyz','t_receipt_offline_financial_report','LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
call xyz.sp_addModify_column('xyz','t_member_agency_settlement','CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
call xyz.sp_addModify_column('xyz','t_member_agency_settlement','LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
call xyz.sp_addModify_column('xyz','t_discount_financial_report','CREATION_BY','varchar(30)','NULL','comment "创建人" ','');
call xyz.sp_addModify_column('xyz','t_discount_financial_report','LAST_UPDATED_BY','varchar(30)','NULL','comment "最后更新人" ','');
call xyz.sp_addModify_column('xyz','t_commission_settlement_record','MEMBER_ACCOUNT','varchar(30)','NULL','comment \'用户账号\' ','after MEMBER_ID');
	
