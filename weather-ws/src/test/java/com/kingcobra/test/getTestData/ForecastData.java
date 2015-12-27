package com.kingcobra.test.getTestData;

import com.kingcobra.kedis.core.RedisConnector;
import redis.clients.jedis.JedisCluster;

import java.util.Map;

/**
 * Created by kingcobra on 15/12/21.
 */
public class ForecastData {
    private static final RedisConnector connector = RedisConnector.Builder.build();
    private JedisCluster jedisCluster = connector.getJedisCluster();
    private static final String pmsc3hTable = "pmsc_3h:%s";
    private static final String pmsc12hTable = "pmsc_12h:%s";

    public void getPmsc3h() {
        String stationId = "3166";
        String key = String.format(pmsc3hTable, stationId);
        Map<String,String> data = jedisCluster.hgetAll(key);
        System.out.println(data);

    }

    public void rmCache() {
        String key = "cache:forecast:101281107.20150731.n:cn";
        jedisCluster.hdel(key, "pmsc_3h_sevenDay1");
    }
    public static void main(String[] args) {
        ForecastData forecastData = new ForecastData();
//        forecastData.getPmsc3h();
        forecastData.rmCache();
    }
}
