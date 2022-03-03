USE xyz;
DELIMITER $$
drop procedure if exists sp_update_members$$
create procedure sp_update_members(V_NEW_LEVEL VARCHAR(50),V_BUNDLE_VERSION varchar(50),V_suffix_ID varchar(20),V_old_new varchar(50))
label1:begin
### DEMO: call sp_update_members('567QP层','qq53.com','a8'); 
### V_NEW_LEVEL:      非必填项 源会员级别名称参数,也可以给NULL或空串(则说明源会员级别名称要和目标会员级别名称一致) 如 '567qp'    
### V_BUNDLE_VERSION: 必填项   要更新的代理层ID字符串参数(即注册来源) 如 'qq53.com'
### V_suffix_ID:      必填项   要加到重复的会员账号后缀串 如 'a8'
### V_old_new:        非必填项 单一的修改旧账号为新账号(用>分隔) 如 'ken123>ken123a1'

### 功能说明(可重复执行):本过程只修改临时库中合并后重复会员的 USER_ACCOUNT,USER_SYSTEM_ID,USER_LEVEL_ID和BUNDLE_VERSION_ID 值
### 本脚只修改 中间库 temp中数据,不能直接修改目标库数据(除下面2表外),后面会进行中间库到目标库的插入合并
###  t_member_user t_commission_settlement_record 此2表比较特殊,需要提前插入目标库中
    declare v_batch_id int default 0; ### 日志表中批次ID值
	declare v_target_increment int default 0; ### 目标中会员表的种子数
    declare v_source_increment int default 0; ### 源中会员表的种子数
	declare v_max              int default 0; ### 源中会员表的最大user_id
    declare v_old_account      varchar(20); ### 旧的会员账号
	declare v_new_account      varchar(20); ### 新的会员账号
	
	set @ti_cnt = 0;
    select IFNULL(count(*),0) into @ti_cnt from xyz.t_merge_db_init where source_table='03.sql' and merge_status = 1;
	IF @ti_cnt < 1 then
	 SELECT '温馨提示：上一步没有成功,请先完成上一步操作!' AS result;
	 LEAVE label1;
	END IF;
    
	IF V_BUNDLE_VERSION is null or V_suffix_ID is null or length(trim(V_BUNDLE_VERSION))=0 or length(trim(V_suffix_ID))=0 then
	 SELECT '温馨提示：传入的2个参数不可为NULL或空串!' AS result;
	 LEAVE label1;
	END IF;
 
    select if(max(batch_id) is null,1,max(batch_id) + 1) into v_batch_id from xyz.t_merge_db_log;
	insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,concat('开始: 04_sp_update_members("',V_NEW_LEVEL,'","',V_BUNDLE_VERSION,'","',V_suffix_ID,'","',V_old_new,'")...'));
	
    ### 当第一次执行时才需要执行下面的更新,多次执行时则不用执行下面的更新操作 
    SELECT  IFNULL(COUNT(*),0) into @cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='xyz_temp' and TABLE_NAME='tmp_member_user';
    IF @cnt = 0 THEN
        
		## V_NEW_LEVEL不为空则合并入统一层级,否则源会员层级和目标会员层级名称要求一样,再通过名称来同步ID	
	    IF V_NEW_LEVEL is not null and length(trim(V_NEW_LEVEL)) > 0 then
			select MEMBER_LEVEL_ID into @MEMBER_LEVEL_ID from xyz.t_member_level where MEMBER_LEVEL_NAME = V_NEW_LEVEL;	
			IF @MEMBER_LEVEL_ID IS NULL then
				 SELECT '温馨提示：传入的会员级别名称在目标库中不存在,请联系产品处理!' AS error;
				 LEAVE label1;		  
			END IF;

		   ## 从目标表查出要导入的会员层级 USER_LEVEL_ID = X,BUNDLE_VERSION_ID = V_BUNDLE_VERSION
		   update xyz_temp.t_member_user set USER_LEVEL_ID = @MEMBER_LEVEL_ID,BUNDLE_VERSION_ID = V_BUNDLE_VERSION;
		ELSE 
		    set @MEMBER_LEVEL_NAME ='';
            select @MEMBER_LEVEL_NAME:=concat(IF(@MEMBER_LEVEL_NAME='','',concat(@MEMBER_LEVEL_NAME,',')),MEMBER_LEVEL_NAME) as level_name from 
			(select MEMBER_LEVEL_NAME from xyz_temp.t_member_level tpml
			   where not exists(select 1 from xyz.t_member_level tml where tml.MEMBER_LEVEL_NAME = tpml.MEMBER_LEVEL_NAME)) tl,
			   (select @MEMBER_LEVEL_NAME ='') tmn;	
			IF @MEMBER_LEVEL_NAME != '' and @MEMBER_LEVEL_NAME is not null then
				 SELECT concat('温馨提示：目标会员层级和源会员层级不一致,缺少这些层级:【',@MEMBER_LEVEL_NAME,'】,请联系产品处理!') AS error;
				 LEAVE label1;		  
			END IF;
			
			update xyz_temp.t_member_user ts inner join xyz_temp.t_member_level tml on ts.USER_LEVEL_ID = tml.MEMBER_LEVEL_ID
			   inner join xyz.t_member_level tm on tml.member_level_name = tm.member_level_name 
			   set ts.USER_LEVEL_ID = tm.MEMBER_LEVEL_ID;
			update xyz_temp.t_member_user set BUNDLE_VERSION_ID = V_BUNDLE_VERSION;
		END IF;
		
		update xyz_member1_temp.`t_member_bundle_version`  set BUNDLE_VERSION_ID = V_BUNDLE_VERSION;
		update xyz_member2_temp.`t_member_bundle_version`  set BUNDLE_VERSION_ID = V_BUNDLE_VERSION;
		update xyz_member3_temp.`t_member_bundle_version`  set BUNDLE_VERSION_ID = V_BUNDLE_VERSION;
		
		update xyz_temp.t_member_bundleversion_report set BUNDLE_VERSION_ID = V_BUNDLE_VERSION;
		
		# t_bet_summary_report 有唯一索引存在,BUNDLE_VERSION_ID 改成一样有可能导致重复,在合并完成后单独初始化
		# update xyz_temp.t_bet_summary_report          set BUNDLE_VERSION_ID = V_BUNDLE_VERSION;
    END IF;
	###########################开始 处理会员表中可能存在重复的会员名称
	create table if not exists xyz_temp.tmp_member_user(old_user_account varchar(30), new_user_account varchar(30),
	                                           CREATION_TIME datetime default CURRENT_TIMESTAMP, LAST_UPDATED_TIME datetime);
											   
	call xyz.sp_create_index('xyz_temp','tmp_member_user',0,'idx_tmu_old','old_user_account');
	call xyz.sp_create_index('xyz_temp','tmp_member_user',0,'idx_tmu_new','new_user_account');
	
	insert into xyz_temp.tmp_member_user(old_user_account)
	select ts.USER_ACCOUNT from xyz_temp.t_member_user ts inner join xyz.t_member_user ta on ts.USER_ACCOUNT = ta.USER_ACCOUNT
	and not exists(select 1 from xyz_temp.tmp_member_user tu where tu.old_user_account = ts.USER_ACCOUNT);		

	select concat("开始更新【tmp_member_user】表,如出现 for key 'idx_tmu_new',请重新传参!") as result;
	
    ### 0.直接修改单一的旧账号为新账号
	if V_old_new is not null and length(trim(V_old_new)) > 0 then
	   set v_old_account:= substring_index(V_old_new,'>',1),v_new_account:= substring_index(V_old_new,'>',-1);
	   if v_old_account='' or v_new_account ='' then
		 SELECT '温馨提示：旧会员账号和新会员账号均不可为空!' AS error;
		 LEAVE label1;		   
	   end if;
	   if length(trim(v_new_account)) < 6 or length(trim(v_new_account)) > 12 then
		 SELECT '温馨提示：新会员账号长度必须是6-12位!' AS error;
		 LEAVE label1;		   
	   end if;
	   SELECT concat('提示：开始修改旧账号为新账号：【',V_old_new,'】...') AS hint;
	   update xyz_temp.tmp_member_user tmp 		   
	   set tmp.new_user_account = v_new_account, tmp.LAST_UPDATED_TIME = now()
	   where tmp.old_user_account = v_old_account and tmp.new_user_account is null
		  and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = v_new_account)
		  and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = v_new_account);
       SELECT concat('提示：结束修改旧账号为新账号：【',V_old_new,'】!!!') AS hint;		  
	end if;
	
    ###	1.超长(大于12位以上)且重复的会员,取前7位+后1位
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,7),right(temp.old_user_account,1)) new_acc from xyz_temp.tmp_member_user temp 
	   where length(temp.old_user_account) > 12 GROUP BY
	   concat(left(temp.old_user_account,7),right(temp.old_user_account,1)) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,7),right(tmp.old_user_account,1))
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,7),right(tmp.old_user_account,1)))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,7),right(tmp.old_user_account,1)))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,7),right(tmp.old_user_account,1)),tmp.LAST_UPDATED_TIME = now();
	
    ###	2.超长(大于12位以上)且重复的会员,取前6位+后2位
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,6),right(temp.old_user_account,2)) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,6),right(temp.old_user_account,2)) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,6),right(tmp.old_user_account,2))
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,6),right(tmp.old_user_account,2)))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,6),right(tmp.old_user_account,2)))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,6),right(tmp.old_user_account,2)),tmp.LAST_UPDATED_TIME = now();
	
    ###	3.超长(大于12位以上)且重复的会员,取前5位+后3位
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,5),right(temp.old_user_account,3)) new_acc from xyz_temp.tmp_member_user temp
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,5),right(temp.old_user_account,3)) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,5),right(tmp.old_user_account,3))
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,5),right(tmp.old_user_account,3)))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,5),right(tmp.old_user_account,3)))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,5),right(tmp.old_user_account,3)),tmp.LAST_UPDATED_TIME = now();

    ###	4.超长(大于12位以上)且重复的会员,取前4位+后4位
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,4),right(temp.old_user_account,4)) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,4),right(temp.old_user_account,4)) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,4),right(tmp.old_user_account,4))
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,4),right(tmp.old_user_account,4)))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,4),right(tmp.old_user_account,4)))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,4),right(tmp.old_user_account,4)),tmp.LAST_UPDATED_TIME = now();
	
    ###	5.超长(大于12位以上)且重复的会员,取前3位+后5位
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,3),right(temp.old_user_account,5)) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,3),right(temp.old_user_account,5)) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,3),right(tmp.old_user_account,5))
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,3),right(tmp.old_user_account,5)))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,3),right(tmp.old_user_account,5)))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,3),right(tmp.old_user_account,5)),tmp.LAST_UPDATED_TIME = now();

    ###	6.超长(大于12位以上)且重复的会员,取前2位+后6位
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,2),right(temp.old_user_account,6)) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,2),right(temp.old_user_account,6)) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,2),right(tmp.old_user_account,6))
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,2),right(tmp.old_user_account,6)))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,2),right(tmp.old_user_account,6)))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,2),right(tmp.old_user_account,6)),tmp.LAST_UPDATED_TIME = now();

    ###	7.超长(大于12位以上)且重复的会员,取前1位+后7位
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,1),right(temp.old_user_account,7)) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,1),right(temp.old_user_account,7)) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,1),right(tmp.old_user_account,7))
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,1),right(tmp.old_user_account,7)))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,1),right(tmp.old_user_account,7)))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,1),right(tmp.old_user_account,7)),tmp.LAST_UPDATED_TIME = now();

    ###	8.超长(大于12位以上)且重复的会员,取前7位+后1位+后缀
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,7),right(temp.old_user_account,1),V_suffix_ID) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,7),right(temp.old_user_account,1),V_suffix_ID) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,7),right(tmp.old_user_account,1),V_suffix_ID)
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,7),right(tmp.old_user_account,1),V_suffix_ID))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,7),right(tmp.old_user_account,1),V_suffix_ID))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,7),right(tmp.old_user_account,1),V_suffix_ID),tmp.LAST_UPDATED_TIME = now();
	
    ###	9.超长(大于12位以上)且重复的会员,取前6位+后2位+后缀
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,6),right(temp.old_user_account,2),V_suffix_ID) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,6),right(temp.old_user_account,2),V_suffix_ID) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,6),right(tmp.old_user_account,2),V_suffix_ID)
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,6),right(tmp.old_user_account,2),V_suffix_ID))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,6),right(tmp.old_user_account,2),V_suffix_ID))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,6),right(tmp.old_user_account,2),V_suffix_ID),tmp.LAST_UPDATED_TIME = now();
			
    ###	10.超长(大于12位以上)且重复的会员,取前5位+后3位+后缀
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,5),right(temp.old_user_account,3),V_suffix_ID) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,5),right(temp.old_user_account,3),V_suffix_ID) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,5),right(tmp.old_user_account,3),V_suffix_ID)
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,5),right(tmp.old_user_account,3),V_suffix_ID))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,5),right(tmp.old_user_account,3),V_suffix_ID))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,5),right(tmp.old_user_account,3),V_suffix_ID),tmp.LAST_UPDATED_TIME = now();

    ###	11.超长(大于12位以上)且重复的会员,取前4位+后4位+后缀
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,4),right(temp.old_user_account,4),V_suffix_ID) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,4),right(temp.old_user_account,4),V_suffix_ID) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,4),right(tmp.old_user_account,4),V_suffix_ID)
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,4),right(tmp.old_user_account,4),V_suffix_ID))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,4),right(tmp.old_user_account,4),V_suffix_ID))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,4),right(tmp.old_user_account,4),V_suffix_ID),tmp.LAST_UPDATED_TIME = now();

    ###	12.超长(大于12位以上)且重复的会员,取前3位+后5位+后缀
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,3),right(temp.old_user_account,5),V_suffix_ID) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,3),right(temp.old_user_account,5),V_suffix_ID) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,3),right(tmp.old_user_account,5),V_suffix_ID)
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,3),right(tmp.old_user_account,5),V_suffix_ID))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,3),right(tmp.old_user_account,5),V_suffix_ID))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,3),right(tmp.old_user_account,5),V_suffix_ID),tmp.LAST_UPDATED_TIME = now();

    ###	13.超长(大于12位以上)且重复的会员,取前2位+后6位+后缀
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,2),right(temp.old_user_account,6),V_suffix_ID) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,2),right(temp.old_user_account,6),V_suffix_ID) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,2),right(tmp.old_user_account,6),V_suffix_ID)
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,2),right(tmp.old_user_account,6),V_suffix_ID))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,2),right(tmp.old_user_account,6),V_suffix_ID))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,2),right(tmp.old_user_account,6),V_suffix_ID),tmp.LAST_UPDATED_TIME = now();

    ###	14.超长(大于12位以上)且重复的会员,取前1位+后7位+后缀
	update xyz_temp.tmp_member_user tmp 
    inner join (select concat(left(temp.old_user_account,1),right(temp.old_user_account,7),V_suffix_ID) new_acc from xyz_temp.tmp_member_user temp 
	where length(temp.old_user_account) > 12  GROUP BY
	   concat(left(temp.old_user_account,1),right(temp.old_user_account,7),V_suffix_ID) HAVING count(*)=1) tpp 
	on tpp.new_acc = concat(left(tmp.old_user_account,1),right(tmp.old_user_account,7),V_suffix_ID)
	   and tmp.new_user_account is null and length(tmp.old_user_account) > 12
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(left(tmp.old_user_account,1),right(tmp.old_user_account,7),V_suffix_ID))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(left(tmp.old_user_account,1),right(tmp.old_user_account,7),V_suffix_ID))		   
	set tmp.new_user_account = concat(left(tmp.old_user_account,1),right(tmp.old_user_account,7),V_suffix_ID),tmp.LAST_UPDATED_TIME = now();
	
    ##########################################################	   
	### 99.正常位数且重复的会员,直接在后面+后缀   
	update xyz_temp.tmp_member_user tmp set tmp.new_user_account = concat(tmp.old_user_account,V_suffix_ID),LAST_UPDATED_TIME = now() 
	where tmp.new_user_account is null and length(tmp.old_user_account) <= 12 
	   and not exists(select 1 from xyz.t_member_user ta where ta.USER_ACCOUNT = concat(tmp.old_user_account,V_suffix_ID))
	   and not exists(select 1 from xyz_temp.t_member_user ts where ts.USER_ACCOUNT = concat(tmp.old_user_account,V_suffix_ID));
	
   select concat("更新【tmp_member_user】表结束!") as result;
	
	### 设置新会员账号还存在重复数量为0
	set @cnt = 0;
	select count(*) into @cnt from xyz_temp.tmp_member_user where new_user_account is null;

	if @cnt > 0 then
	    select concat('温馨提示：会员表中重复的会员加后缀为:【',V_suffix_ID,'】后还有重复的 数量为:',@cnt) as ERROR;
	else
	   update xyz_temp.t_member_user ts inner join xyz_temp.tmp_member_user tmp on ts.USER_ACCOUNT = tmp.old_user_account
	     set ts.USER_ACCOUNT = tmp.new_user_account, 
		 ts.USER_SYSTEM_ID = REPLACE(ts.USER_SYSTEM_ID,CONCAT('-',ts.USER_ACCOUNT),concat('-',tmp.new_user_account)),
		 ts.LAST_UPDATED_TIME = now();

	   update xyz_temp.t_commission_settlement_record ts inner join xyz_temp.tmp_member_user tmp on ts.MEMBER_ACCOUNT = tmp.old_user_account
	     set ts.MEMBER_ACCOUNT = tmp.new_user_account, ts.LAST_UPDATED_TIME = now();
		
		### 此处提前插入目标t_member_user会员表,对此表进行特殊处理,此处 ID为主键且为种子,但要提前插入目标库中
		select IFNULL(COUNT(*),0),KeyValue_Add into @cnt,@USER_ID from xyz.t_merge_db_init where merge_status = 0 and target_table ='xyz.t_member_user';
		if @cnt = 1 then
            ### 先插入日志信息
			insert into xyz.t_merge_db_log(batch_id,memo) 
			  values(v_batch_id,'Begin 同步表【xyz.t_member_user】(这里如果出现 Duplicate entry 出现重复会员的话,请重新执行本过程)...');	
            SELECT '温馨提示:开始对目标表【xyz.t_member_user】同步数据(这里如果出现 Duplicate entry 出现重复会员的话,请重新执行本过程)...' as result;			
			insert into xyz.t_member_user(USER_ID,DATASOURCE_KEY,USER_SYSTEM_ID,PROXY_LINK_CODE,PARENT_ID,USER_LEVEL_ID,USER_ACCOUNT,USER_PASSWORD,REGISTER_IP,REGISTER_REAL_ADDRESS,REGISTER_SOURCE,REGISTER_SOURCE_NAME,SEX,HEAD_PORTRAIT,BIRTHDAY,NICK_NAME,EMAIL,USER_NAME,TELEPHONE,USER_QQ,USER_WX,TEAM_COUNTS,IS_HAVE_SUBORDINATE,IS_OWNER,IS_ENABLE,IS_FROZEN,APP_UUID,BUNDLE_VERSION_ID,REMARK,LOGIN_FREQUENCY,LOGIN_TIME,LOGIN_IP,LOGIN_REAL_ADDRESS,LOGIN_SOURCE,CREATION_TIME,CREATION_BY,LAST_UPDATED_TIME,LAST_UPDATED_BY) 
			select USER_ID + @USER_ID,DATASOURCE_KEY,USER_SYSTEM_ID,concat(fn_NumContent_delimiter('_',@USER_ID,PROXY_LINK_CODE)),PARENT_ID + @USER_ID,USER_LEVEL_ID,USER_ACCOUNT,USER_PASSWORD,REGISTER_IP,REGISTER_REAL_ADDRESS,REGISTER_SOURCE,REGISTER_SOURCE_NAME,SEX,HEAD_PORTRAIT,BIRTHDAY,NICK_NAME,EMAIL,USER_NAME,TELEPHONE,USER_QQ,USER_WX,TEAM_COUNTS,IS_HAVE_SUBORDINATE,IS_OWNER,IS_ENABLE,IS_FROZEN,APP_UUID,BUNDLE_VERSION_ID,REMARK,LOGIN_FREQUENCY,LOGIN_TIME,LOGIN_IP,LOGIN_REAL_ADDRESS,LOGIN_SOURCE,CREATION_TIME,CREATION_BY,LAST_UPDATED_TIME,LAST_UPDATED_BY  from xyz_temp.t_member_user;
            ### 对会员表插入完成后,要马上把其种子数在原基础上调整,以防止源库中其他相关表删除的脏USER_ID影响目标库的相关表记录
			select auto_increment into v_target_increment from information_schema.tables where table_schema='xyz' and table_name='t_member_user';
			select auto_increment into v_source_increment from information_schema.tables where table_schema='xyz_temp' and table_name='t_member_user';
			select max(user_id)   into v_max from xyz_temp.t_member_user;

			set @sql=concat('alter table xyz.t_member_user auto_increment = ',v_target_increment + round((v_source_increment - v_max + 10000)/10000)*10000,';');
			PREPARE STMT FROM @SQL;
			EXECUTE STMT;	
			DEALLOCATE PREPARE STMT;				
		    insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,'End 同步表【xyz.t_member_user】完成!!!');	
			
			update xyz.t_merge_db_init set merge_status = 1 where target_table ='xyz.t_member_user';
			SELECT '温馨提示:对目标表【xyz.t_member_user】同步数据处理完成!' as result;				
		end if;
		
		### 此处提前插入目标t_commission_settlement_record 对此表进行特殊处理,此处ID为主键但不是种子,只能人为维护ID
		select IFNULL(COUNT(*),0) into @cnt from xyz.t_merge_db_init where merge_status = 0 and target_table ='xyz.t_commission_settlement_record';
		if @cnt = 1 then	
			select  IFNULL(min(id), 1000000) INTO @min_id from xyz.t_commission_settlement_record;

            ### 先插入日志信息			
			insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,'Begin 同步表【xyz.t_commission_settlement_record】...');	
			select '温馨提示:开始对目标表【xyz.t_commission_settlement_record】同步数据...' as result;
			insert into xyz.t_commission_settlement_record(ID,MEMBER_ID,MEMBER_ACCOUNT,PROXY_LINK_CODE,HIERARCHY_NAME,BATCH,CONTRIBUTIONS,DAMA_OWNER,DAMA_SUB,DAMA_TEAM,DAMA_VALUE,OWNER_PERFORMANCE,TEAM_PERFORMANCE,CONTRIBUTE_COMMISSION,ALL_COMMISSION,PROXYMODE_CODE,SELF_SATISFIED,STATUS,CREATION_TIME,CREATION_BY,LAST_UPDATED_TIME,LAST_UPDATED_BY,PARENT_ID) select (@id:=@id - 1),MEMBER_ID + @USER_ID,MEMBER_ACCOUNT,concat(fn_NumContent_delimiter('_',@USER_ID,PROXY_LINK_CODE)),HIERARCHY_NAME,BATCH,CONTRIBUTIONS,DAMA_OWNER,DAMA_SUB,DAMA_TEAM,DAMA_VALUE,OWNER_PERFORMANCE,TEAM_PERFORMANCE,CONTRIBUTE_COMMISSION,ALL_COMMISSION,PROXYMODE_CODE,SELF_SATISFIED,STATUS,CREATION_TIME,CREATION_BY,LAST_UPDATED_TIME,LAST_UPDATED_BY,PARENT_ID + @USER_ID  from xyz_temp.t_commission_settlement_record, (select @id := @min_id) t;
			insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,'End 同步表【xyz.t_commission_settlement_record】完成!!!');
						
			update t_merge_db_init set merge_status = 1 where target_table ='xyz.t_commission_settlement_record';
			select '温馨提示:对目标表【xyz.t_commission_settlement_record】同步数据处理完成!' as result;

		END IF;
		
	   update xyz.t_merge_db_init set merge_status = 1, last_updated_time = now() where source_table='04.sql';
	   insert into xyz.t_merge_db_log(batch_id,memo) values(v_batch_id,concat('结束: 04_sp_update_members("',V_NEW_LEVEL,'","',V_BUNDLE_VERSION,'","',V_suffix_ID,'","',V_old_new,'")!!!'));
       select '成功提示:更新临时库中重复会员相关数据成功,请进行【下一步】操作!' as result;	
	end if;
	###########################结束 处理会员表中可能存在重复的会员名称	
		
end $$
DELIMITER ;

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
call xyz.sp_addModify_column('xyz_temp','t_member_bet_statistics','CREATION_BY','varchar(30)','NULL','comment \'创建人\' ','');
call xyz.sp_addModify_column('xyz_temp','t_member_bet_statistics','LAST_UPDATED_BY','varchar(30)','NULL','comment \'创建人\' ','');
call xyz.sp_addModify_column('xyz_temp','t_game_type_total','CREATION_BY','varchar(30)','NULL','comment \'创建人\' ','');
call xyz.sp_addModify_column('xyz_temp','t_game_type_total','LAST_UPDATED_BY','varchar(30)','NULL','comment \'创建人\' ','');

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
call xyz.sp_addModify_column('xyz','t_member_bet_statistics','CREATION_BY','varchar(30)','NULL','comment \'创建人\' ','');
call xyz.sp_addModify_column('xyz','t_member_bet_statistics','LAST_UPDATED_BY','varchar(30)','NULL','comment \'创建人\' ','');
call xyz.sp_addModify_column('xyz','t_game_type_total','CREATION_BY','varchar(30)','NULL','comment \'创建人\' ','');
call xyz.sp_addModify_column('xyz','t_game_type_total','LAST_UPDATED_BY','varchar(30)','NULL','comment \'创建人\' ','');

### 开始更新会员表数据 sp_update_members(V_NEW_LEVEL,V_BUNDLE_VERSION,V_suffix_ID,V_old_new); 四个参数说明
### V_NEW_LEVEL:      非必填项 目标端新的会员层级   如 'tengxunqp'    
### V_BUNDLE_VERSION: 必填项   目标端注册来源       如 '6796.com'
### V_suffix_ID:      必填项   重复的会员账号后缀串 如 'a1'
### V_old_new:        非必填项 单一的修改旧账号为新账号(用>分隔) 如 'ken123>ken123a1'

call xyz.sp_update_members('tengxunqp','6796.com','a1','');

