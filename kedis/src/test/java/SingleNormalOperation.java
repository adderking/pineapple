import redis.clients.jedis.*;

import java.util.Map;

/**
 * Created by kingcobra on 15/8/10.
 */
public class SingleNormalOperation {
    private JedisPool jedisPool = RedisConnection.makePool();

    public void setDataToSet() {
        Jedis jedis = jedisPool.getResource();

        jedis.sadd("user:1:tags", "man", "soldier");
        System.out.println(jedis.smembers("user:1:tags"));
    }

    public void testConnectionHandler() {
        JedisClusterConnectionHandler connectionHandler = RedisConnection.getConnectionHandler();
//        Jedis jedis = connectionHandler.getConnectionFromNode(new HostAndPort("hadoop6", 6379));
        Jedis jedis = connectionHandler.getConnectionFromNode(new HostAndPort("192.168.1.114", 6379));
//        jedis.set("a", "1");
        jedis.incr("a");
        System.out.println(jedis.get("a"));
        Map<String,JedisPool> _poolMap = connectionHandler.getNodes();
        for(Map.Entry<String,JedisPool> entry : _poolMap.entrySet()){
            System.out.println(entry.getKey());
            JedisPool _pool = entry.getValue();
            Jedis j = _pool.getResource();
            System.out.println(j.clusterInfo());
        }

    }
    public static void main(String[] args) {
        SingleNormalOperation singleNormalOperation = new SingleNormalOperation();
        singleNormalOperation.setDataToSet();
        singleNormalOperation.testConnectionHandler();
    }
}
