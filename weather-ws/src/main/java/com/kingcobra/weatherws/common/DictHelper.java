package com.kingcobra.weatherws.common;

import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kingcobra on 15/12/18.
 * 缓存气象字典数据
 */
@Component("dictHelper")
public class DictHelper {
    public static final Map<String, JSONObject> weatherLevelCache = new HashMap<String, JSONObject>();
    public static final Map<String, JSONObject> ffLevelCache = new HashMap<String, JSONObject>();
    public static final Map<String, JSONObject> ddLevelCache = new HashMap<String, JSONObject>();

    private final RedisConnector redisConnector;
    private final JedisCluster jedisCluster;


    public DictHelper() {
        this.redisConnector = RedisConnector.Builder.build();
        this.jedisCluster = redisConnector.getJedisCluster();
        initalDdLevelCache();
        initialFfLevelCache();
        initialWeatherLevelCache();
    }

    private void initialWeatherLevelCache() {
        String dictName = Constant.DICT_WEATHER;
        parseDictValue(dictName, weatherLevelCache);
    }

    private void initialFfLevelCache() {
        String dictName = Constant.DICT_FF;
        parseDictValue(dictName, ffLevelCache);
    }

    private void initalDdLevelCache() {
        String dictName = Constant.DICT_DD;
        parseDictValue(dictName, ddLevelCache);
    }

    private void parseDictValue(String dictTableName, Map<String, JSONObject> cache) {
        Map<String, String> dictValue = jedisCluster.hgetAll(dictTableName);
        String key;
        JSONObject value;
        for (Map.Entry<String, String> entry : dictValue.entrySet()) {
            key = entry.getKey();
            value = JSONObject.parseObject(entry.getValue());
            cache.put(key, value);
        }
    }
    /**
     * 根据天气现象等级获得描述信息
     * @param level 天气现象等级
     * @param language  语言
     * @return 描述信息
     */
    public String weatherLevelParser(String level,Constant.Language language) {
        JSONObject descObject =   DictHelper.weatherLevelCache.get(level);
        String desc = null;
        switch (language) {
            case CN:
                desc= descObject.getString("name");break;
            case EN:
                desc= descObject.getString("ename");break;
        }
        return desc;
    }

    /**
     * 风力现象描述信息
     * @param level
     * @param language
     * @return
     */
    public String ffLevelParser(String level,Constant.Language language) {
        JSONObject descObject =   DictHelper.ffLevelCache.get(level);
        String desc = null;
        switch (language) {
            case CN:
                desc= descObject.getString("name");break;
            case EN:
                desc= descObject.getString("ename");break;
        }
        return desc;
    }
    /**
     * 风力现象描述信息
     * @param level
     * @param language
     * @return
     */
    public String ddLevelParser(String level,Constant.Language language) {
        JSONObject descObject =   DictHelper.ddLevelCache.get(level);
        String desc = null;
        switch (language) {
            case CN:
                desc= descObject.getString("name");break;
            case EN:
                desc= descObject.getString("ename");break;
        }
        return desc;
    }
}
