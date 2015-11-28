package com.kingcobra.flume;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.flume.monitor.AbstractEventMonitor;
import com.kingcobra.flume.monitor.PmscParser;
import com.kingcobra.flume.monitor.StationMonitor;
import com.kingcobra.kedis.core.RedisConnector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Created by kingcobra on 15/9/28.
 */
public class TestMonitor {

    private static final RedisConnector REDIS_CONNECTOR = RedisConnector.Builder.build();
    @Test
    public void initStations() {
        JedisCluster jedisCluster = REDIS_CONNECTOR.getJedisCluster();
        String key = "external.stations";
        jedisCluster.del(key);

    }
    @Test
    public void testGetMonitorResult() {
        String keyFormat = "monitor:%s";
        String datatype = "forecast3h.external";
        JedisCluster jedisCluster = REDIS_CONNECTOR.getJedisCluster();
        String key = String.format(keyFormat, datatype);
        List<String> result = jedisCluster.lrange(key, 0, -1);
        for (String r : result) {
            System.out.println(r);
            JSONObject jsonObject = JSON.parseObject(r);
            long timestamp = jsonObject.getLong("timestamp");
            Date d = new Date(timestamp);
            System.out.println(d);
            JSONArray array = jsonObject.getJSONArray("missStations");
            System.out.println("array size: "+array.size());
        }
        getStationsCount();
        keyFormat = "monitor:%s";
        datatype = "pmsc";
        String key1 = String.format(keyFormat, datatype);
        List<String> result1 = jedisCluster.lrange(key1, 0, -1);
        for (String s : result1) {
            System.out.println(result1.get(0));
            JSONObject jsonObject = JSON.parseObject(result1.get(0));
            long timestamp = jsonObject.getLong("timestamp");
            Date d = new Date(timestamp);
            System.out.println(d);
            JSONArray array = jsonObject.getJSONArray("missStations");
            System.out.println("array size: " + array.size());
        }
        getStationsCount();
    }
    @Test
    public void getStationsCount() {
        JedisCluster jedisCluster = REDIS_CONNECTOR.getJedisCluster();
        String stations = "external.stations";
        Set<String> stationSet=jedisCluster.smembers(stations);
        System.out.println(stationSet.size());
    }
    @After
    public void closeRedisConnector() {
        REDIS_CONNECTOR.closeJedisCluster();
    }


}
