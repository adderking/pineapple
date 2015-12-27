package com.kingcobra.test.initializer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.test.MyAppConfig;
import com.kingcobra.weatherws.WebAppInitializer;
import com.kingcobra.weatherws.WebConfig;
import com.kingcobra.weatherws.common.Constant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kingcobra on 15/12/7.
 */
public class BusinessRuleInitializer {
    private static final RedisConnector connector = RedisConnector.Builder.build();
    private JedisCluster jedisCluster = connector.getJedisCluster();

    public void initialize() {
        String businessRuleName = "pmsc_3h_sevenDay1";
        String key = String.format(Constant.BUSINESSRULE_TABLE, businessRuleName);
        Map<String, String> rule = new HashMap<String, String>();
        rule.put(Constant.BUSINESSRULE_DBTYPE, "redis");
        rule.put(Constant.BUSINESSRULE_DATATABLENAME, "pmsc_3h");
        rule.put(Constant.BUSINESSRULE_TIMECOLUMN, "LST");
//        rule.put(Constant.BUSINESSRULE_COLUMNS, "[\"stationId\",\"lon\",\"lat\",\"height\",\"TEMP\",\"TEMP_MAX\",\"TEMP_MIN\",\"RAIN\",\"FF\",\"FF_LEVEL\",\"DD\",\"DD_LEVEL\",\"CLOUD\",\"WEATHER\",\"RH\"]");
        rule.put(Constant.BUSINESSRULE_COLUMNS, "[\"LST\",\"TEMP\",\"TEMP_MAX\",\"TEMP_MIN\",\"FF_LEVEL\",\"DD_LEVEL\",\"WEATHER\"]");
        rule.put(Constant.BUSINESSRULE_ISAROUNDAREA, "false");
        rule.put(Constant.BUSINESSRULE_ISAROUNDTRAVEL, "false");
        rule.put(Constant.BUSINESSRULE_ISWEEKEND, "false");
        rule.put(Constant.BUSINESSRULE_AREATYPE, "internal");
        rule.put(Constant.BUSINESSRULE_STARTTIME, "now");
        rule.put(Constant.BUSINESSRULE_ENDTIME, "now+7");
        String result =   jedisCluster.hmset(key, rule);
        System.out.println(result);
    }
    public void weekendBusinessRule() {
        String businessRuleName = "pmsc_3h_weekend";
        String key = String.format(Constant.BUSINESSRULE_TABLE,businessRuleName);
        Map<String, String> rule = new HashMap<String, String>();
        rule.put(Constant.BUSINESSRULE_DBTYPE, "redis");
        rule.put(Constant.BUSINESSRULE_DATATABLENAME, "pmsc_3h");
        rule.put(Constant.BUSINESSRULE_TIMECOLUMN, "LST");
//        rule.put(Constant.BUSINESSRULE_COLUMNS, "[\"stationId\",\"lon\",\"lat\",\"height\",\"TEMP\",\"TEMP_MAX\",\"TEMP_MIN\",\"RAIN\",\"FF\",\"FF_LEVEL\",\"DD\",\"DD_LEVEL\",\"CLOUD\",\"WEATHER\",\"RH\"]");
        rule.put(Constant.BUSINESSRULE_COLUMNS, "[\"LST\",\"TEMP\",\"TEMP_MAX\",\"TEMP_MIN\",\"FF_LEVEL\",\"DD_LEVEL\",\"WEATHER\"]");
        rule.put(Constant.BUSINESSRULE_ISAROUNDAREA, "false");
        rule.put(Constant.BUSINESSRULE_ISAROUNDTRAVEL, "false");
        rule.put(Constant.BUSINESSRULE_ISWEEKEND, "true");
        rule.put(Constant.BUSINESSRULE_AREATYPE, "internal");
        rule.put(Constant.BUSINESSRULE_STARTTIME, "now");
        rule.put(Constant.BUSINESSRULE_ENDTIME, "now+7");
        String result = jedisCluster.hmset(key, rule);
        System.out.println(result);
    }
    public void getRule(String businessRuleName) {
        String key = String.format(Constant.BUSINESSRULE_TABLE, businessRuleName);
        Map<String,String> values = jedisCluster.hgetAll(key);
        JSONObject rule = (JSONObject)JSONObject.toJSON(values);
        String array = rule.getString(Constant.BUSINESSRULE_COLUMNS);
        System.out.println(array);
    }

    public static void main(String[] args) {
        BusinessRuleInitializer businessRuleInitializer = new BusinessRuleInitializer();
//        businessRuleInitializer.initialize();
        businessRuleInitializer.weekendBusinessRule();
        businessRuleInitializer.getRule("pmsc_3h_weekend");
    }
}
