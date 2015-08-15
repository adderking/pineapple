package com.kingcobra.kedis.core;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by kingcobra on 15/8/13.
 */
public class RedisConnector {
    private static JedisPool jedisPool = null;
    private static JedisCluster jedisCluster = null;
    private static final JedisPoolConfig JEDIS_POOL_CONFIG = new JedisPoolConfig();

    static{
        JEDIS_POOL_CONFIG.setMaxTotal(10000);
        JEDIS_POOL_CONFIG.setMaxIdle(1000);
        JEDIS_POOL_CONFIG.setMaxWaitMillis(5000);


    }
    private  RedisConnector() {

    }

    public synchronized static JedisPool getJedisPool(String masterIP,int port) {
        if (jedisPool == null) {
            jedisPool = new JedisPool(JEDIS_POOL_CONFIG, masterIP, port);
        }
        return jedisPool;
    }

    public synchronized static JedisCluster getJedisCluster() {
        if (jedisCluster == null) {
            jedisCluster = new JedisCluster(Constant.RMASTER_HOST_AND_PORT, JEDIS_POOL_CONFIG);
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
