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
 *
 */
public class CsvInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return null;
    }

    @Override
    public void close() {

    }
}
