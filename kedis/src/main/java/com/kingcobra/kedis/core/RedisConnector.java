package com.kingcobra.kedis.core;

import com.alibaba.fastjson.JSONArray;
import com.kingcobra.kedis.util.JedisUtils;
import redis.clients.jedis.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by kingcobra on 15/8/13.
 */
public class RedisConnector {
    private static JedisPool jedisPool = null;
    private static JedisCluster jedisCluster = null;
    private static final JedisPoolConfig JEDIS_POOL_CONFIG = new JedisPoolConfig();
    private static Properties properties;

    static{
        JEDIS_POOL_CONFIG.setMaxTotal(10000);
        JEDIS_POOL_CONFIG.setMaxIdle(1000);
        JEDIS_POOL_CONFIG.setMaxWaitMillis(5000);

    }
    private RedisConnector() {

    }

    public synchronized static JedisPool getJedisPool(String masterIP,int port) {
        if (jedisPool == null) {
            jedisPool = new JedisPool(JEDIS_POOL_CONFIG, masterIP, port);
        }
        return jedisPool;
    }

    public synchronized static JedisCluster getJedisCluster() {
        if (jedisCluster == null) {
            properties = new Properties();
            try {
                InputStream inputStream = RedisConnector.class.getClassLoader().getResourceAsStream("system.properties");;
                properties.load(inputStream);
                String nodes = properties.getProperty("redis.cluster.nodes");
                String ports = properties.getProperty("redis.cluster.ports");
                JSONArray a_nodes = JSONArray.parseArray(nodes);
                JSONArray a_ports = JSONArray.parseArray(ports);
                Set<HostAndPort> rmaster_host_and_port = new HashSet<>();
                for (int i = 0; i < a_nodes.size(); i++) {
                    for (int j = 0; j < a_ports.size(); j++) {
                        HostAndPort hostAndPort = new HostAndPort(a_nodes.getString(i), a_ports.getInteger(j));
                        rmaster_host_and_port.add(hostAndPort);
                    }
                }
                jedisCluster = new JedisCluster(rmaster_host_and_port, JEDIS_POOL_CONFIG);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return jedisCluster;
    }

    public synchronized static void closeJedisPool() {
        if (jedisPool != null) {
            jedisPool.close();
            jedisPool.destroy();
            jedisPool = null;
        }
    }
    public synchronized static void closeJedisCluster() {
        if (jedisCluster != null) {
            jedisCluster.close();
            jedisCluster = null;
        }
    }
}
