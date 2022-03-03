USE xyz_temp;
DELIMITER $$
drop procedure if exists sp_check_table_sum$$
create procedure sp_check_table_sum()
begin
### 此过程主要用于检查配置表和临时库数量是否一致!!!

            select count(*) into @init_cnt from xyz.t_merge_db_init where source_table not in('03.sql','04.sql','05.sql','06.sql'); 
			select count(*) into @db_cnt   from information_schema.tables where TABLE_SCHEMA in ('xyz_temp','xyz_member1_temp','xyz_member2_temp','xyz_member3_temp');
			
			select if(max(batch_id) is null,1,max(batch_id) + 1) into @v_batch_id from xyz.t_merge_db_log;					
			IF @init_cnt = @db_cnt and @init_cnt > 0 then
			   insert into xyz.t_merge_db_log(batch_id,memo) values
			     (@v_batch_id, concat('成功提示:配置表数量:【',@init_cnt,'】和临时库TEMP中数量【',@db_cnt,'】一致,请进行下一步操作!'));
			   SELECT concat('成功提示:配置表数量:【',@init_cnt,'】和临时库TEMP中数量【',@db_cnt,'】一致,请进行下一步操作!') as result;
			   update xyz.t_merge_db_init set merge_status = 1, last_updated_time = now() where source_table='03.sql';
			ELSE
			   insert into xyz.t_merge_db_log(batch_id,memo) values
			     (@v_batch_id, concat('ERROR:配置表数量:【',@init_cnt,'】和临时库TEMP中数量【',@db_cnt,'】不一致,请检查修复后再重试!'));			
			   SELECT concat('ERROR:配置表数量:【',@init_cnt,'】和临时库TEMP中数量【',@db_cnt,'】不一致,请检查修复后再重试!') as error;
			END IF;
end $$
DELIMITER ;

call sp_check_table_sum;