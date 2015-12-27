package com.kingcobra.weatherws.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by kingcobra on 15/12/7.
 * 区域（AreaId)的帮助类，提供不同的查询方法。
 */
@Component("areaHelper")
public class AreaHelper {
    private final RedisConnector redisConnector;
    private final JedisCluster jedisCluster;

    public AreaHelper() {
        redisConnector = RedisConnector.Builder.build();
        jedisCluster = redisConnector.getJedisCluster();
    }

    /**
     * 通过areaId获得对应的区域信息
     * @param tableName redis表名:国内表，国外表，景点表
     * @param areaId
     * @param columns 需要查询的字段，不指定查询所有信息
     * @return 保存area信息的JSON对象
     */
    public JSONObject findAreaInfo(String tableName,String areaId,String... columns) {
        String key = String.format(tableName, areaId);
        String[] column = columns;
        JSONObject areaInfo = new JSONObject();
        if(column.length==0) {
            Map<String, String> area = jedisCluster.hgetAll(key);
            areaInfo = (JSONObject) JSONObject.toJSON(area);
        }else {
            List<String> area = jedisCluster.hmget(key,columns);
            for (int i = 0; i < area.size(); i++) {
                areaInfo.put(column[i], area.get(i));
            }
        }
        return areaInfo;
    }


    /**
     * 通过areaId查找周边区域对应的areaId和stationId
     * @param tableName 国内周边区域表，国外周边区域表，国内景点表
     * @param areaId
     * @return 返回JSONArray,数组元素为JSON对象，格式{areaId:,stationId:}
     */
    public JSONArray findAroundArea(String tableName,String areaId) {
        String key = String.format(tableName, areaId);
        Set<String> aroundAreas = jedisCluster.zrange(key, 0, -1);
        JSONArray arrayAroundAreas = new JSONArray();
        for (String s : aroundAreas) {
            JSONObject _o = JSONObject.parseObject(s);
            arrayAroundAreas.add(_o);
        }
        return arrayAroundAreas;
    }
}
