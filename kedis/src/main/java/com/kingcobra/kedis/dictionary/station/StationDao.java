package com.kingcobra.kedis.dictionary.station;

import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.DBConstant;
import com.kingcobra.kedis.core.RedisConnector;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by kingcobra on 15/8/14.
 * dict_station table DAO
 */
public class StationDao {
    private static final JedisCluster jedisCluster = RedisConnector.getJedisCluster();

    public long saveStation(JSONObject jsonObject) {
        Long result=0l;
        if(jsonObject.containsKey("areaId")) {
            String areaId = jsonObject.getString("areaId");
            String key = String.format(DBConstant.PREFIX_STATION_KEY, areaId);
            Set<Map.Entry<String, Object>> sets = jsonObject.entrySet();
            for (Map.Entry<String, Object> entry : sets) {
               result= jedisCluster.hset(key, entry.getKey(), entry.getValue().toString());
            }
            return result;
        }
        return -1l;
    }

    public void saveStationAround() {
        String key = String.format(DBConstant.PREFIX_STATIONAROUND_KEY, "areaId");
        Map<String, Double> aroundStations = new HashMap<String, Double>();
        aroundStations.put("{stationId:1,areaId:1,aroundStationId:2,aroundAreaId:2}", 100d);
        aroundStations.put("{stationId:1,areaId:1,aroundStationId:3,aroundAreaId:3}",99d);
        jedisCluster.zadd(key, aroundStations);

    }

    public void getData() {
       Map<String,String> values =  jedisCluster.hgetAll(String.format(DBConstant.PREFIX_STATION_KEY, "areaId"));
        System.out.println(values);
        for (Map.Entry<String, String> entry : values.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
        Set<String> sValues = jedisCluster.zrange(String.format(DBConstant.PREFIX_STATIONAROUND_KEY, "areaId"), 0l, -1l);
        for (String s : sValues) {
            System.out.println(s);
        }
    }
    public static void main(String[] args) {
        StationDao stationDao = new StationDao();
        stationDao.saveStation(null);
        stationDao.saveStationAround();
        stationDao.getData();
    }

}
