SET @SS='';
select @SS:=CONCAT(@SS,COLUMN_NAME, ',') AS aa FROM (
	 select COLUMN_NAME from information_schema.COLUMNS t 
	 inner join (select distinct PLATFORM_CODE  from t_member_bet_report) tbt 
	 on substring_index(t.COLUMN_NAME,'_PERSON',1) = tbt.PLATFORM_CODE
	 where t.TABLE_SCHEMA='xyz' and t.TABLE_NAME='T_PLATFORM_BET_REPORT_tmp'  
	 and t.COLUMN_NAME not in ('id','REPORT_DATE','CREATION_TIME','CREATION_BY','LAST_UPDATED_TIME','LAST_UPDATED_BY') 
	 order by substring_index(t.COLUMN_NAME,'_PERSON',1) ) tt;
SET @SS=CONCAT(@SS,'REPORT_DATE',',CREATION_TIME',',CREATION_BY',',LAST_UPDATED_TIME',',LAST_UPDATED_BY');  
SET @SS=CONCAT(' INSERT INTO T_PLATFORM_BET_REPORT_tmp(',@SS,') ');

SET @EE='';
select @EE:=CONCAT(@EE,'sum(if(PLATFORM_CODE= \'',PLATFORM_CODE,'\',user_count,0)) as ',PLATFORM_CODE, ',') AS aa FROM (select PLATFORM_CODE from  (select distinct PLATFORM_CODE  from t_member_bet_report) tmb
  where  tmb.PLATFORM_CODE not in ('id','REPORT_DATE','CREATION_TIME','CREATION_BY','LAST_UPDATED_TIME','LAST_UPDATED_BY')
	AND EXISTS( select 1 from information_schema.COLUMNS t 
	 where substring_index(t.COLUMN_NAME,'_PERSON',1) = tmb.PLATFORM_CODE
	 and t.TABLE_SCHEMA='xyz' and t.TABLE_NAME='T_PLATFORM_BET_REPORT_tmp' ) ) ta
ORDER BY PLATFORM_CODE ;

SET @QQ = CONCAT(@SS,' select ',@EE,' report_date,now(),\'system\',now(),\'system\' FROM (
select report_date,PLATFORM_CODE,COUNT(user_id) as user_count from t_member_bet_report group BY report_date,PLATFORM_CODE  
) t group by report_date  order by report_date ');
PREPARE stmt FROM @QQ;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- create table  T_PLATFORM_BET_REPORT_tmp like T_PLATFORM_BET_REPORT;