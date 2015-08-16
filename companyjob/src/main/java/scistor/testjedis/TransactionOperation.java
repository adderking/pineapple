package scistor.testjedis;

import redis.clients.jedis.*;

/**
 * Created by kingcobra on 15/8/6.
 */
public class TransactionOperation {
    private JedisCluster jedisCluster = RedisConnection.initCluster();
    private JedisPool jedisPool = RedisConnection.makePool();
    private ShardedJedisPool shardedJedisPool = RedisConnection.connectCluster();
    private Jedis jedis;
    public void t_set() {
        jedis = jedisPool.getResource();
        Transaction transaction = jedis.multi();
        transaction.set("a", "1");
//        transaction.set("c", "2");
        transaction.incr("a");
        transaction.exec();
    }

    public void t_get() {
        jedis = jedisPool.getResource();
        String value  = jedis.get("a");
        System.out.println(value);
    }
    /*
       使用shardedJedis 连接集群
       redis cluster不支持事务
     */
    public void t_c_start() {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        shardedJedis.set("user", "mike");
        System.out.println(shardedJedis.get("user"));
    }

    public static void main(String[] args) {
        TransactionOperation transactionOperation = new TransactionOperation();
//        transactionOperation.t_set();
//        transactionOperation.t_get();
        transactionOperation.t_c_start();
    }
}
