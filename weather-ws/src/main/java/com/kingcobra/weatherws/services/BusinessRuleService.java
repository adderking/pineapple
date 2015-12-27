package com.kingcobra.weatherws.services;

import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.weatherws.common.Constant;
import com.kingcobra.weatherws.exceptions.WrongBusinessNameException;
import com.kingcobra.weatherws.exceptions.WrongTimeRangeException;
import com.kingcobra.weatherws.utils.RegexUtils;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisCluster;

import java.util.Map;

/**
 * Created by kingcobra on 15/12/6.
 *
 * 业务规则内容
 * dbType:‘redis’，		//数据库类型：hbase or redis
 * tableName:"pmsc_fine",	//数据表名
 * timeColumn:,	//设定描述数据预报时间的列，
 * columns:[],		//需要查询的数据列，jsonArray格式
 * isInternal:true	//true:国内,false:国外
 * //以下三个配置在每个接口标准中只能出现一个，三选一
 * isWeekend:true,	//是否为周末，如果为true，不需要设置startTime和endTime。
 * isAroundArea:true,	//是否查找周边区域
 * isAroundTravel:true,	//是否查找周边景点
 * startTime: now+1 //now代表从当前日期开始，数值为天数
 * endTime: now+7
 *
 */
@Service("businessRuleService")
public class BusinessRuleService {
    private final RedisConnector redisConnector;
    private final JedisCluster jedisCluster;

    public BusinessRuleService() {
        redisConnector = RedisConnector.Builder.build();
        jedisCluster = redisConnector.getJedisCluster();
    }

    /**
     * find business rule
     * @param businessName
     * @return
     */
    public JSONObject findBusinessRule(String businessName){
        String tableName = String.format(Constant.BUSINESSRULE_TABLE, businessName);
        Map<String,String> ruleMap =jedisCluster.hgetAll(tableName);
        if(ruleMap==null || ruleMap.size()==0)
            throw new WrongBusinessNameException(businessName);
        JSONObject rule = (JSONObject) JSONObject.toJSON(ruleMap);
        checkRule(rule);
        return rule;
    }

    /**
     * check the format of businessRule
     * @param rule
     */
    private void checkRule(JSONObject rule) {
        String startTime = rule.getString(Constant.BUSINESSRULE_STARTTIME);
        String endTime = rule.getString(Constant.BUSINESSRULE_ENDTIME);
        boolean isRight = RegexUtils.checkTimeInRule(startTime);
        if(!isRight)
            throw new WrongTimeRangeException(startTime);
        isRight = RegexUtils.checkTimeInRule(endTime);
        if(!isRight)
            throw new WrongTimeRangeException(endTime);

    }
}
