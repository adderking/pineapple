package com.kingcobra.flume.interceptor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.kingcobra.flume.util.StringBloomFilter;
import com.kingcobra.kedis.core.RedisConnector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by kingcobra on 15/9/13.
 * 采集每次传来的数据中包含的stationId，用于判断每次缺失的区域数据。
 */
public class CsvInterceptor implements Interceptor {
    private final RedisConnector redisConnector;
    private final Integer key; //气象站点位置索引
    private final String dataType;  //数据类型，用于jedis保存集合的key。
    private final String stationsTable;
    private static final String JEDIS_KEY = "monitor:%s";
    private String currentStationId;
    private final JedisCluster jedisCluster;
    private StringBloomFilter stringBloomFilter;

    private CsvInterceptor(Integer key, String dataType,String stationsTable) {
        String nodes = "[\"192.168.1.112:6378\",\"192.168.1.113:6378\",\"192.168.1.114:6378\",\"192.168.1.109:6378\",\"192.168.1.110:6378\",\"192.168.1.111:6378\",\"192.168.1.112:6379\",\"192.168.1.113:6379\",\"192.168.1.114:6379\",\"192.168.1.109:6379\",\"192.168.1.110:6379\",\"192.168.1.111:6379\"]";
        this.key = key;
        this.dataType = dataType;
        this.stationsTable = stationsTable;
        redisConnector = RedisConnector.Builder.build(nodes,"1000","500","5000");
        jedisCluster = redisConnector.getJedisCluster();

    }
    @Override
    public void initialize() {
        stringBloomFilter = new StringBloomFilter(500000, 0.01d);
    }

    @Override
    public Event intercept(Event event) {
        String lineContent = new String(event.getBody());
        String stationId = lineContent.split(",")[key];
        stringBloomFilter.put(stationId);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            this.intercept(event);
        }
        return list;
    }

    @Override
    public void close() {
        redisConnector.closeJedisCluster();
    }

    public static class Builder implements Interceptor.Builder {
        private Integer key;
        private String dataType;
        private String stations_table;
        @Override
        public Interceptor build() {
            return new CsvInterceptor(key,dataType,stations_table);
        }

        @Override
        public void configure(Context context) {
            this.key =context.getInteger(Constant.KEY);
            this.dataType = context.getString(Constant.DATA_TYPE);
            this.stations_table = context.getString(Constant.STATIONS_TABLE);
        }
    }

    private class Constant {
        private static final String KEY = "monitor.key";
        private static final String DATA_TYPE = "monitor.dataType";
        private static final String STATIONS_TABLE = "monitor.stations";
    }
}
