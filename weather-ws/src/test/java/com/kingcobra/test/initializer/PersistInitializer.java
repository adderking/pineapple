package com.kingcobra.test.initializer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by kingcobra on 15/12/21.
 */
public class PersistInitializer {
    private static final RedisConnector redisConnector = RedisConnector.Builder.build();
    private static final JedisCluster jedisCluster = redisConnector.getJedisCluster();
    private static final String persistRuleName = "persistPolicy:%s";
    private static final String persistTableName = "persistTarget:%s";
    /**
     * 初始化持久化规则
     */
    public void persistRuleInitial() {
        //pmsc_fine_3h_15day
        JSONArray type = JSONArray.parseArray("[{\"name\":\"redis\",\"params\":{\"recordIdentifier\":\"TIME_STEP\",\"key\":\"stationId\"}}]");
        JSONArray columns = JSONArray.parseArray("[\"stationId\",\"lon\",\"lat\",\"height\",\"$t\",\"$t\",\"$t\",\"$t\",\"$t\",\"$t\",\"TIME_STEP\",\"UTC\",\"LST\",\"TEMP\",\"TEMP_MAX\",\"TEMP_MIN\",\"RAIN\",\"FF\",\"FF_LEVEL\",\"DD\",\"DD_LEVEL\",\"CLOUD\",\"WEATHER\",\"RH\"]");
        Map<String,String> rule_3h = new HashMap<String, String>();
        rule_3h.put("columns", columns.toJSONString());
        rule_3h.put("type", type.toJSONString());
        String key = String.format(persistRuleName, "pmsc_fine_3h");
        String status = jedisCluster.hmset(key, rule_3h);

        //pmsc_fine_12h
        columns= JSONArray.parseArray("[\"stationId\",\"lon\",\"lat\",\"height\",\"$t\",\"$t\",\"$t\",\"$t\",\"$t\",\"$t\",\"TIME_STEP\",\"UTC\",\"TEMP\",\"TEMP_MAX\",\"TEMP_MIN\",\"RAIN\",\"FF\",\"FF_LEVEL\",\"DD\",\"DD_LEVEL\",\"CLOUD\",\"WEATHER\",\"RH\"]");
        Map<String, String> rule_12h = new HashMap<String, String>();
        rule_12h.put("columns", columns.toJSONString());
        rule_12h.put("type", type.toJSONString());
        String key_12h = String.format(persistRuleName, "pmsc_fine_12h");
        status += jedisCluster.hmset(key_12h, rule_12h);
        System.out.println(status);
    }

    /**
     * 查看持久化规则
     */
    public void getPersistRule() {
        String persistRuleName = "pmsc_fine_3h";    //pmsc_fine_3h_15day
        String key = String.format(PersistInitializer.persistRuleName, persistRuleName);
        Map<String,String> rule = jedisCluster.hgetAll(key);
        System.out.println(rule);
        persistRuleName = "pmsc_fine_12h";
        key = String.format(PersistInitializer.persistRuleName, persistRuleName);
        rule = jedisCluster.hgetAll(key);
        System.out.println(rule);
    }

    /**
     * 初始化持久化目标表
     */
    public void persistTableInitial() {
        String topic = "pmsc_3h",topic_12h = "pmsc_12h";
        Map<String, String> data = new HashMap<String, String>();
        data.put("redis", "pmsc_3h");
        data.put("rule", "pmsc_fine_3h");
        String key = String.format(persistTableName, topic);
        String status = jedisCluster.hmset(key, data);
        Map<String, String> data_12h = new HashMap<String, String>();
        data_12h.put("redis", "pmsc_12h");
        data_12h.put("rule", "pmsc_fine_12h");
        String key_12h = String.format(persistTableName, topic_12h);
        status += jedisCluster.hmset(key_12h, data_12h);
        System.out.println(status);

    }

    /**
     * 查看持久化目标表
     */
    public void getPersistTable() {
        String topic = "pmsc_3h";
        String key = String.format(persistTableName, topic);
        Map<String,String> value = jedisCluster.hgetAll(key);
        System.out.println(value);
        String topic_12h = "pmsc_12h";
        key = String.format(persistTableName, topic_12h);
        value = jedisCluster.hgetAll(key);
        System.out.println(value);

    }

    public static void main(String[] args) {
        PersistInitializer persistInitializer = new PersistInitializer();
//        persistInitializer.persistRuleInitial();
        persistInitializer.getPersistRule();
//        persistInitializer.persistTableInitial();
        persistInitializer.getPersistTable();
    }
}
