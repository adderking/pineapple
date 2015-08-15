package com.kingcobra.kedis.core;

import redis.clients.jedis.JedisCluster;

/**
 * Created by kingcobra on 15/8/13.
 */
public abstract class JedisClusterOperation {
    private String key;
    private static final JedisCluster JEDISCLUSTER = RedisConnector.getJedisCluster();
    public void setKey(String key){
        this.key = key;
    }

    public String setStringType(String key,String value) {
       return  JEDISCLUSTER.set(key, value);
    }

    public void setHashType() {

    }
}
