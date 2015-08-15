import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by kingcobra on 15/8/4.
 */
public class RedisConnection {
    private static JedisPool jedisPool=null;
    private static JedisCluster jedisCluster=null;
    private static ShardedJedisPool shardedJedisPool = null;    //面向集群的jedis pool
    private static final Set<HostAndPort> HOSTS = new HashSet<HostAndPort>();
    public static synchronized JedisPool makePool() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(10000);
            jedisPoolConfig.setMaxIdle(1000);
            jedisPoolConfig.setMaxWaitMillis(5000);
            jedisPoolConfig.setTestOnBorrow(true);
//            jedisPool = new JedisPool(jedisPoolConfig, "hadoop4",6379);
            jedisPool = new JedisPool(jedisPoolConfig, "192.168.1.112",6379);
        }
        return jedisPool;

    }

    public static synchronized JedisCluster initCluster() {
        if(jedisCluster==null) {
            /*HOSTS.add(new HostAndPort("hadoop4", 6378));
            HOSTS.add(new HostAndPort("hadoop4", 6379));
            HOSTS.add(new HostAndPort("hadoop6", 6379));*/
            HOSTS.add(new HostAndPort("192.168.1.112", 6378));
            HOSTS.add(new HostAndPort("192.168.1.112", 6379));
            HOSTS.add(new HostAndPort("192.168.1.114", 6379));
            jedisCluster = new JedisCluster(HOSTS);
        }
        return jedisCluster;

    }

    public static synchronized ShardedJedisPool connectCluster() {
        if (shardedJedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(10000);
            jedisPoolConfig.setMaxIdle(1000);
            jedisPoolConfig.setMaxWaitMillis(5000);
            List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
           /* shards.add(new JedisShardInfo("hadoop4", 6378));
            shards.add(new JedisShardInfo("hadoop4", 6379));
            shards.add(new JedisShardInfo("hadoop6", 6379));*/
            shards.add(new JedisShardInfo("192.168.1.112", 6378));
            shards.add(new JedisShardInfo("192.168.1.112", 6379));
            shards.add(new JedisShardInfo("192.168.1.114", 6379));
            shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
        }
        return shardedJedisPool;
    }

    public static synchronized JedisClusterConnectionHandler getConnectionHandler() {
        JedisPoolConfig config = new JedisPoolConfig();
        /*HOSTS.add(new HostAndPort("hadoop4", 6378));
        HOSTS.add(new HostAndPort("hadoop4", 6379));
        HOSTS.add(new HostAndPort("hadoop6", 6379));*/
        HOSTS.add(new HostAndPort("192.168.1.112", 6378));
        HOSTS.add(new HostAndPort("192.168.1.112", 6379));
        HOSTS.add(new HostAndPort("192.168.1.114", 6379));
        JedisClusterConnectionHandler jedisClusterConnectionHandler = new JedisSlotBasedConnectionHandler(HOSTS, config, 5000);
        return jedisClusterConnectionHandler;
    }


    public static void main(String[] args) {
        RedisConnection redisConnection = new RedisConnection();
        redisConnection.initCluster();
    }
}
