use 8bet;
DELIMITER $$
DROP PROCEDURE IF EXISTS  `sp_findParent`$$
DROP FUNCTION IF EXISTS  `fn_findParent`$$
CREATE FUNCTION `fn_findParent`(`subId` int)
RETURNS varchar(4000) DETERMINISTIC
BEGIN
DECLARE sTemp VARCHAR(4000);
DECLARE sTempPar VARCHAR(4000);
SET sTemp = '';
SET sTempPar = subId;
#循环递归
WHILE sTempPar is not null DO
    #判断是否是第一个，不加的话第一个会为空
    IF sTemp != '' THEN
        SET sTemp = concat(sTemp,',',sTempPar);
    ELSE
        SET sTemp = sTempPar;
    END IF;
    SET sTemp = concat(sTemp,',',sTempPar);
    SELECT group_concat(parent_id) INTO sTempPar FROM 8bet.t_proxy_summary_report where parent_id<>user_id and FIND_IN_SET(user_id,sTempPar)>0; 
END WHILE;
RETURN sTemp;
END $$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS  `sp_findSubNode`$$
DROP FUNCTION IF EXISTS  `fn_findSubNode`$$
CREATE FUNCTION `fn_findSubNode`(`orgid` int) 
RETURNS varchar(4000) DETERMINISTIC
BEGIN
DECLARE oTemp VARCHAR(4000);
DECLARE oTempChild VARCHAR(4000);
SET oTemp = '';
SET oTempChild = orgid;
WHILE oTempChild IS NOT NULL DO
    #判断是否是第一个，不加的话第一个会为空
    IF oTemp != '' THEN
        SET oTemp = concat(oTemp,',',oTempChild);
    ELSE
        SET oTemp = oTempChild;
    END IF;
	SET oTemp = CONCAT(oTemp,',',oTempChild);
	SELECT GROUP_CONCAT(user_id) INTO oTempChild FROM 8bet.t_proxy_summary_report WHERE FIND_IN_SET(parent_id,oTempChild)>0;
END WHILE;
RETURN oTemp;
END $$
DELIMITER ;