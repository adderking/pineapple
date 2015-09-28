package com.kingcobra.kedis.util;

import com.kingcobra.kedis.core.RedisConnector;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/9/7.
 */
public class JedisUtils {
    private static final JedisPoolConfig JEDIS_POOL_CONFIG = new JedisPoolConfig();
    /**
     * get master nodes in the cluster currently
     * @param ip random node 's IP
     * @param port
     * @return
     */
    public static Set<HostAndPort> getMaster(String ip, int port) {
        JedisPool jedisPool = new JedisPool(JEDIS_POOL_CONFIG, ip, port);
        Jedis jedis = jedisPool.getResource();
        String nodes = jedis.clusterNodes();
        String s = "([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}:[0-9]{4} )(myself,)?(master)";
        Pattern pattern = Pattern.compile(s);
        Matcher matcher = pattern.matcher(nodes);
        Set<HostAndPort> masters = new HashSet<HostAndPort>();
        HostAndPort hostAndPort;
        String[] ipAndPort;
        while (matcher.find()) {
            String master = nodes.substring(matcher.start(), matcher.end() - 7);
            master = master.replaceAll(" myself", "");
            System.out.println(master);
            ipAndPort = master.split(":");
            hostAndPort = new HostAndPort(ipAndPort[0], Integer.valueOf(ipAndPort[1]));
            masters.add(hostAndPort);
        }
        return masters;
    }
}
