package com.kingcobra.kedis.core;

import com.alibaba.fastjson.JSONArray;
import com.kingcobra.kedis.util.JedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by kingcobra on 15/8/13.
 */
public class RedisConnector {
    private JedisPool jedisPool = null;
    private JedisCluster jedisCluster = null;
    private JedisPoolConfig JEDIS_POOL_CONFIG = new JedisPoolConfig();
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnector.class);

    private Lock lock = new ReentrantLock();

    private Set<HostAndPort> hostAndPorts;

    private RedisConnector(Set<HostAndPort> hostAndPorts,int maxTotal,int maxIdel,long maxWaitMillis) {
        JEDIS_POOL_CONFIG.setMaxTotal(maxTotal);
        JEDIS_POOL_CONFIG.setMaxIdle(maxIdel);
        JEDIS_POOL_CONFIG.setMaxWaitMillis(maxWaitMillis);
        this.hostAndPorts=hostAndPorts;
    }

    public JedisPool getJedisPool(String ip, int port) {
        synchronized (lock) {
            if (jedisCluster == null) {
                this.getJedisCluster();
            }
            if (jedisPool == null) {
                String key = ip + ":" + port;
                Map<String, JedisPool> jedisPools = jedisCluster.getClusterNodes();
                jedisPool = jedisPools.get(key);
            }
            return jedisPool;
        }
    }
    public JedisCluster getJedisCluster() {
        synchronized (lock) {
            if (jedisCluster == null) {
                jedisCluster = new JedisCluster(hostAndPorts, JEDIS_POOL_CONFIG);
            }
            return jedisCluster;
        }
    }

    public void closeJedisPool() {
        synchronized (lock) {
            if (jedisPool != null) {
                jedisPool.close();
                jedisPool.destroy();
                jedisPool = null;
            }
        }

    }
    public void closeJedisCluster() {
        synchronized (lock) {
            if (jedisCluster != null) {
                jedisCluster.close();
                jedisCluster = null;
            }
        }
    }
    public static class Builder {
        /**
         * 从RedisConnector类加载器运行的JVM加载Properties
         * @return
         */
        public static RedisConnector build() {
            Properties properties = new Properties();
            try {
                InputStream inputStream = RedisConnector.class.getClassLoader().getResourceAsStream("system.properties");
                properties.load(inputStream);
            }catch(IOException ioException){
                LOGGER.error(ioException.getMessage());
            }
            return build(properties);
        }

        /**
         * 从配置文件加载Redis集群信息,Properties文件格式
         * redis.cluster.nodes=["192.168.1.112:6378","192.168.1.112:6379","192.168.1.113:6378","192.168.1.113:6379"]
         * @param properties
         * @return
         */
        public static RedisConnector build(Properties properties) {
            String nodes = properties.getProperty("redis.cluster.nodes");
            String maxTotal = properties.getProperty(Constant.MAXTOTAL, Constant.MAXTOTAL_DEFAULT);
            String maxIdel = properties.getProperty(Constant.MAXIDEL, Constant.MAXIDEL_DEFAULT);
            String maxWaitMillis = properties.getProperty(Constant.MAXWAITMILLIS, Constant.MAXWAITMILLIS_DEFAULT);
            return build(nodes,maxTotal,maxIdel,maxWaitMillis);
        }

        /**
         *  hostAndPort style likes nodes in properties file.
         * @param hostAndPort
         * @param maxTotal
         * @param maxIdel
         * @param maxWaitMillis
         * @return
         */
        public static RedisConnector build(String hostAndPort,String maxTotal,String maxIdel,String maxWaitMillis) {
            Set<HostAndPort> host_and_port = parseString(hostAndPort);
            return build(host_and_port,maxTotal,maxIdel,maxWaitMillis);
        }

        /**
         *
         * @param hostAndPorts
         * @param maxTotal
         * @param maxIdel
         * @param maxWaitMillis
         * @return
         */
        public static RedisConnector build(Set<HostAndPort> hostAndPorts,String maxTotal,String maxIdel,String maxWaitMillis) {
            int maxTotal$ = Integer.parseInt(maxTotal);
            int maxIdel$ = Integer.parseInt(maxIdel);
            long maxWaitMillis$ = Long.parseLong(maxWaitMillis);
            return new RedisConnector(hostAndPorts,maxTotal$,maxIdel$,maxWaitMillis$);
        }

        /**
         * @param hostandPort
         * @return
         */
        private static Set<HostAndPort> parseString(String hostandPort) {
            JSONArray a_nodes = JSONArray.parseArray(hostandPort);
            Set<HostAndPort> host_and_port = new HashSet<>();
            String[] a_HostAndPort;
            for (int i = 0; i < a_nodes.size(); i++) {
                a_HostAndPort = a_nodes.getString(i).split(":");
                HostAndPort hostAndPort = new HostAndPort(a_HostAndPort[0],Integer.parseInt(a_HostAndPort[1]));
                host_and_port.add(hostAndPort);
            }
            return host_and_port;
        }

        public class Constant {
            public static final String MAXTOTAL = "jedis.maxTotal";
            public static final String MAXIDEL = "jedis.maxIdel";
            public static final String MAXWAITMILLIS = "jedis.maxWaitMillis";
            public static final String MAXTOTAL_DEFAULT = "10000";
            public static final String MAXIDEL_DEFAULT = "1000";
            public static final String MAXWAITMILLIS_DEFAULT = "5000";

        }
    }
}
