use xyz;
DROP TABLE IF EXISTS t_merge_db_init;
CREATE TABLE t_merge_db_init(  FID               int not null auto_increment primary key comment '主键ID(自增)',
							   source_table      varchar(200) comment '源  【库名.表名】',
							   target_table      varchar(200) comment '目标【库名.表名】', 
							   merge_status      tinyint default 0 comment '是否合并成功 <=0-否,1-是',
							   is_batch          bit default 0 comment '是否要批量合并数据(06最后一步) 0-否(默认,一次性合并),1-是(批量合并)',
							   force_check       bit default 1 comment '是否要进行强类型一致检查 0-否,1-是',
							   init_type         tinyint default 0 comment '0-只需复制到新库; 1-关键ID需要加某个数值(比如100000)等操作,然后再复制到新库',        
							   sum_type          bit default 0 comment '0-不需要累加; 1-需要累加(没找到则新增,找到则更新累加)',
							   sum_field         varchar(500)  comment '需要累加的字段名(以英文半角逗号分隔),没有则为NULL',
							   sum_field_by_key  varchar(500)  comment '需要累加所依赖的唯一键(以英文半角逗号分隔,调用过程创建唯一索引),没有则为NULL',
							   KeyValue_Add      int default null comment '需要在USER_ID上累加的一个整数值(能被1万整除的数),可由MAX(USER_ID)+10000取整获取',
							   creation_by       varchar(20) default 'sys' comment '创建人',
							   creation_time     datetime default CURRENT_TIMESTAMP comment '创建时间',
							   last_updated_time datetime default NULL comment '修改时间'
);
alter table t_merge_db_init comment '合并数据库前存储将要进行合并的字典信息表';

## 创建3个唯一索引保证,源和目标表唯一且一一对应
call sp_create_index('xyz','t_merge_db_init',0,'idx_tmdi_ts','source_table');
call sp_create_index('xyz','t_merge_db_init',0,'idx_tmdi_ta','target_table');
call sp_create_index('xyz','t_merge_db_init',0,'idx_tmdi_st','source_table,target_table');

#### 此4行记录03-06步骤是否成功与否的状态
insert into t_merge_db_init(source_table,target_table,merge_status) values('03.sql','03.sql',-1);
insert into t_merge_db_init(source_table,target_table,merge_status) values('04.sql','04.sql',-1);
insert into t_merge_db_init(source_table,target_table,merge_status) values('05.sql','05.sql',-1);
insert into t_merge_db_init(source_table,target_table,merge_status) values('06.sql','06.sql',-1);

CREATE TABLE IF NOT EXISTS t_merge_db_log(FID      int not null auto_increment primary key comment '主键ID(自增)',
                                          batch_id int comment '批次ID',
                                          memo     varchar(2000) comment '备注',
										  sql_memo     text comment 'SQL备注',
                                          source_table     varchar(200) comment '源  【库名.表名】',
										  batch_status bit default 1 comment '批次状态 0-异常 1-正常',
                                          source_field_error  varchar(2000) comment '源字段列表(包含类型,顺序,数量),以英文半角逗号分隔;无异常则为NULL',         
                                          target_field_error  varchar(2000) comment '目标字段列表(包含类型,顺序,数量),以英文半角逗号分隔;无异常则为NULL',  
										  creation_by         varchar(20) default 'sys' comment '创建人',
										  creation_time       datetime default CURRENT_TIMESTAMP comment '创建时间'							
);
alter table t_merge_db_log comment '合并数据库的日志信息表';
call sp_create_index('xyz','t_merge_db_log',1,'idx_t_merge_db_log_id','batch_id');

### MEMBER_ID  MEMBER_ACCOUNT USER_ID USER_ACCOUNT proxy_link_code  USER_SYSTEM_ID  PARENT_ID  ORDER_CODE(-会员ID+时间戳)
### RECHARGE_ORDER_CODE(修改ID+10W DEMO:R4-6155520191008233300762 )   PLATFROM_ORDER_CODE(+后缀编码)
### start xyz;
set @_xyz ='xyz_temp';
set @_xyz_member1 ='xyz_member1_temp';
set @_xyz_member2 ='xyz_member2_temp';
set @_xyz_member3 ='xyz_member3_temp';

#### 设置分月表向前只取几个月的数据(最小为1)
set @forward_months = 3;
set @start_month = DATE_FORMAT(now()- interval @forward_months - 1 month,'%Y%m');

############################# 以下为分月表(16个)
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_withdraw_order_',@start_month),concat('xyz.t_withdraw_order_',@start_month),1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_proxy_summary_report_',@start_month),concat('xyz.t_proxy_summary_report_',@start_month),1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_recharge_order_',@start_month),concat('xyz.t_recharge_order_',@start_month),1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_account_change_',@start_month),concat('xyz.t_account_change_',@start_month),1,1,0,NULL,NULL);
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member1,'.t_member_account_change_',@start_month),concat('xyz_member1.t_member_account_change_',@start_month),1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member1,'.t_member_platform_bet_',@start_month),concat('xyz_member1.t_member_platform_bet_',@start_month),1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member1,'.t_member_recharge_order_',@start_month),concat('xyz_member1.t_member_recharge_order_',@start_month),1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member1,'.t_member_withdraw_',@start_month),concat('xyz_member1.t_member_withdraw_',@start_month),1,1,0,NULL,NULL);
	
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member2,'.t_member_account_change_',@start_month),concat('xyz_member2.t_member_account_change_',@start_month),1,1,0,NULL,NULL);				 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member2,'.t_member_platform_bet_',@start_month),concat('xyz_member2.t_member_platform_bet_',@start_month),1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member2,'.t_member_recharge_order_',@start_month),concat('xyz_member2.t_member_recharge_order_',@start_month),1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member2,'.t_member_withdraw_',@start_month),concat('xyz_member2.t_member_withdraw_',@start_month),1,1,0,NULL,NULL);
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member3,'.t_member_account_change_',@start_month),concat('xyz_member3.t_member_account_change_',@start_month),1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member3,'.t_member_recharge_order_',@start_month),concat('xyz_member3.t_member_recharge_order_',@start_month),1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member3,'.t_member_platform_bet_',@start_month),concat('xyz_member3.t_member_platform_bet_',@start_month),1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member3,'.t_member_withdraw_',@start_month),concat('xyz_member3.t_member_withdraw_',@start_month),1,1,0,NULL,NULL);
					 
############################# 以上为分月表(16个)	
	
#### t_member_level 可能会使用到,但不需要同步,所以此处插入时就为merge_status=1(合并成功),
insert into t_merge_db_init(source_table,target_table,merge_status,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_level'),'xyz.t_member_level',1,1,0,0,NULL,NULL);				 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_admin_device'),'xyz.t_admin_device',1,0,0,NULL,NULL);	
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_return_point_temp'),'xyz.t_return_point_temp',1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_agency_account_change'),'xyz.t_agency_account_change',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_agent_download_summary_report'),'xyz.t_agent_download_summary_report',1,0,1,
					  ',IOS_DOWNLOAD_COUNT,AN_DOWNLOAD_COUNT,IOS_MEMBER_COUNT,AN_MEMBER_COUNT,H5_MEMBER_COUNT,',
					  'BUNDLE_VERSION_ID,REPORT_DATE');
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_bet_summary_report'),'xyz.t_bet_summary_report',1,1,0,NULL,NULL);

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_commission_settlement_record'),'xyz.t_commission_settlement_record',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_commission_summary_report'),'xyz.t_commission_summary_report',1,1,0,NULL,NULL);

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_company_report'),'xyz.t_company_report',1,0,0,					 ',new_member_count,sum_balance,pay_amount,pay_person_count,pay_count,withdraw_amount,withdraw_person_count,withdraw_count,preferential_amount,preferential_person_count,preferential_count,gift_amount,gift_person_count,gift_count,washing_code_amount,washing_code_person_count,washing_code_count,bet_amount,bet_person_count,bet_count,net_amount,net_person_count,net_count,first_deposit_person_count,first_deposit_recharge_amount,company_pay_off,',
					 'REPORT_DATE');
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_db_member'),'xyz.t_db_member',1,0,1,',MEMBER_COUNT,',
					 'DATASOURCE_KEY');

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_discount_financial_report'),'xyz.t_discount_financial_report',1,0,0,
					 ',preferential_amount,preferential_count,preferential_number,','discount_type,REPORT_DATE');
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_discount_report'),'xyz.t_discount_report',1,1,0,NULL,NULL);					 

					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_discount_summary_report'),'xyz.t_discount_summary_report',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_equipment_stat_report'),'xyz.t_equipment_stat_report',1,0,1,
					 ',REGISTER_NUM,REG_RECHARGE_NUM,REG_RECHARGE_SUM,RECHARGE_NUM,RECHARGE_SUM,','STAT_TIME,PACKAGE_CODE,EQUIPMENT_TYPE');	

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_game_type_total'),'xyz.t_game_type_total',1,0,0,
					 ',BET_COUNT,BET_AMOUNT,PAY_AMOUNT,WIN_LOS_AMOUNT,GAME_JOIN_COUNT,JOIN_GAME_HOT_PROPORTION,','BUNDLE_VERSION_ID,GAMING_TYPE,PLATFORM_CODE,REPORT_DATE,GAME_ITEM_CODE');
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_manual_recharge_summary_report'),'xyz.t_manual_recharge_summary_report',1,1,0,NULL,NULL);	

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_manual_withdraw_summary_report'),'xyz.t_manual_withdraw_summary_report',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_agency_daily_settlement'),'xyz.t_member_agency_daily_settlement',1,1,0,NULL,NULL);	

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_agency_repeti_settlement'),'xyz.t_member_agency_repeti_settlement',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_agency_settlement'),'xyz.t_member_agency_settlement',1,1,0,NULL,NULL);	

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_bankcard'),'xyz.t_member_bankcard',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_bet_report'),'xyz.t_member_bet_report',1,1,0,NULL,NULL);

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_bet_statistics'),'xyz.t_member_bet_statistics',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_bundleversion_report'),'xyz.t_member_bundleversion_report',1,1,0,NULL,NULL);					 

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_discount_log'),'xyz.t_member_discount_log',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_integral_log'),'xyz.t_member_integral_log',1,1,0,NULL,NULL);	
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_pay_log'),'xyz.t_member_pay_log',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_platform_bet'),'xyz.t_member_platform_bet',1,1,0,NULL,NULL);	
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_promotion_gold'),'xyz.t_member_promotion_gold',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_recharge_log'),'xyz.t_member_recharge_log',1,1,0,NULL,NULL);	
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_recharge_report'),'xyz.t_member_recharge_report',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_recharge_summary_report'),'xyz.t_member_recharge_summary_report',1,1,0,NULL,NULL);	
					 					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_rp_log'),'xyz.t_member_rp_log',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_salary_log'),'xyz.t_member_salary_log',1,1,0,NULL,NULL);	
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_sign_config'),'xyz.t_member_sign_config',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_sign_log'),'xyz.t_member_sign_log',1,1,0,NULL,NULL);	
	
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_user'),'xyz.t_member_user',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_member_withdraw_log'),'xyz.t_member_withdraw_log',1,1,0,NULL,NULL);					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_payment_out_report'),'xyz.t_payment_out_report',1,1,0,NULL,NULL);
			 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_proxy_wallet'),'xyz.t_proxy_wallet',1,1,0,NULL,NULL);	

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_receipt_offline_financial_report'),'xyz.t_receipt_offline_financial_report',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.T_PAY_RECHARGE_FINANCIAL_REPORT'),'xyz.T_PAY_RECHARGE_FINANCIAL_REPORT',1,1,0,NULL,NULL);
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_receipt_offline_report'),'xyz.t_receipt_offline_report',1,0,0,
					 ',PAY_PERSON_COUNT,PAY_COUNT,PAY_AMOUNT,','RECEIPT_ACCOUNT,REPORT_DATE,RECHARGE_TYPE');	
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_recharge_summary_report'),'xyz.t_recharge_summary_report',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_return_point_summary_report'),'xyz.t_return_point_summary_report',1,1,0,NULL,NULL);	
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_shareholder_member_report'),'xyz.t_shareholder_member_report',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_shareholder_report'),'xyz.t_shareholder_report',1,0,0,					 ',MEMBER_COUNT,NEW_ANDROID_MEMBER_COUNT,ANDROID_MEMBER_COUNT,NEW_H5_MEMBER_COUNT,H5_MEMBER_COUNT,NEW_IOS_MEMBER_COUNT,IOS_MEMBER_COUNT,BET_COUNT,BET_AMOUNT,NET_AMOUNT,SUM_BALANCE,PAY_AMOUNT,WITHDRAW_AMOUNT,WASHING_CODE_AMOUNT,COMMISSION_AMOUNT,ARTIFICIAL_PAY_AMOUNT,ARTIFICIAL_WITHDRAW_AMOUNT,',
					 'BUNDLE_VERSION_ID,REPORT_DATE');	
	
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_total_commission_log'),'xyz.t_total_commission_log',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz,'.t_withdraw_summary_report'),'xyz.t_withdraw_summary_report',1,1,0,NULL,NULL);	

### end xyz;

### start xyz_member1
### t_member_platform_registered 此表导入会导致三方登录异常,故注释了 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member1,'.t_member_bundle_version'),'xyz_member1.t_member_bundle_version',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member1,'.t_member_inspection'),'xyz_member1.t_member_inspection',1,1,0,NULL,NULL);
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member1,'.t_member_safe_deposit'),'xyz_member1.t_member_safe_deposit',1,1,0,NULL,NULL);

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member1,'.t_member_safe_deposit_detail'),'xyz_member1.t_member_safe_deposit_detail',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member1,'.t_member_wallet'),'xyz_member1.t_member_wallet',1,1,0,NULL,NULL);
					 
### end  xyz_member1

### start xyz_member2
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member2,'.t_member_bundle_version'),'xyz_member2.t_member_bundle_version',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member2,'.t_member_inspection'),'xyz_member2.t_member_inspection',1,1,0,NULL,NULL);
					 
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member2,'.t_member_safe_deposit'),'xyz_member2.t_member_safe_deposit',1,1,0,NULL,NULL);

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member2,'.t_member_safe_deposit_detail'),'xyz_member2.t_member_safe_deposit_detail',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member2,'.t_member_wallet'),'xyz_member2.t_member_wallet',1,1,0,NULL,NULL);
				 
### end  xyz_member2

### start xyz_member3	
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member3,'.t_member_bundle_version'),'xyz_member3.t_member_bundle_version',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member3,'.t_member_inspection'),'xyz_member3.t_member_inspection',1,1,0,NULL,NULL);					 

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member3,'.t_member_safe_deposit'),'xyz_member3.t_member_safe_deposit',1,1,0,NULL,NULL);

insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member3,'.t_member_safe_deposit_detail'),'xyz_member3.t_member_safe_deposit_detail',1,1,0,NULL,NULL);
insert into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key) 
                     values(concat(@_xyz_member3,'.t_member_wallet'),'xyz_member3.t_member_wallet',1,1,0,NULL,NULL);
					 
### end  xyz_member3

#####################增加所有到当前日期的分月表
USE xyz;
DELIMITER $$
drop procedure if exists sp_add_init$$
create procedure sp_add_init()
begin
### 此过程主要用于配置表中分月表的动态插入功能

      declare done int default false;	
			declare v_schema varchar(20);
			declare v_table_name varchar(100);
            declare v_target_table varchar(100);			
			declare v_min_date char(6);
      declare v_max_date char(6) default date_format(now(),'%Y%m');	
			
			declare my_cur cursor for select target_table, substring_index(target_table,'.',1), left(substring_index(target_table,'.',-1),
			       length(substring_index(target_table,'.',-1))-6), right(target_table,6) from t_merge_db_init  where target_table like '%_20%' 
				   group by left(target_table,length(target_table)-6);
		
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;
			open my_cur;
			read_loop:loop
			fetch my_cur into v_target_table, v_schema, v_table_name, v_min_date;
					IF done then
							leave read_loop;
						END IF;
						
					### 最小分月的下月赋值给当前值
					set @cur_date = date_format(date_format(concat(v_min_date,'01'),'%Y%m%d') + interval 1 month,'%Y%m');
					WHILE @cur_date <= v_max_date DO
						 insert ignore into t_merge_db_init(source_table,target_table,force_check,init_type,sum_type,sum_field,sum_field_by_key)
						 select concat(v_schema,'_temp.',v_table_name,@cur_date), concat(v_schema,'.',v_table_name,@cur_date), force_check, init_type,
						        sum_type, sum_field, sum_field_by_key  from t_merge_db_init where target_table = v_target_table;	
						 set @cur_date = date_format(date_format(concat(@cur_date,'01'),'%Y%m%d') + interval 1 month,'%Y%m');
					END WHILE;		
			end loop;
			close my_cur;
			
end $$
DELIMITER ;				

call sp_add_init;
drop procedure if exists sp_add_init;

#### 取得要合并到的会员表中最大的会员ID+10000后的追加值,且写入t_merge_db_init表中
update xyz.t_merge_db_init set KeyValue_Add = (select round((max(user_id)+10000)/10000)*10000 from xyz.t_member_user);

select '初始化配置数据成功,请进行下一步操作!' as result;
