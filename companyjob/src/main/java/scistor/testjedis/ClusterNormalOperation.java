package scistor.testjedis;

import redis.clients.jedis.JedisCluster;

/**
 * Created by kingcobra on 15/8/6.
 */
public class ClusterNormalOperation {
    private JedisCluster jedisCluster = RedisConnection.initCluster();
    public void saveData() {
        jedisCluster.set("testKey", "hello world");
        jedisCluster.set("testKey1", "hello world 1");
        jedisCluster.hset("user:1", "name", "kingcobra");

    }

    public void getData() {
        String value = jedisCluster.get("testKey");
        System.out.println(value);
        value = jedisCluster.get("testKey1");
        value = jedisCluster.hget("user:1", "name");
        System.out.println(value);
    }

    public void delData() {
        jedisCluster.del("testKey");
        jedisCluster.del("testKey1");
        String value = jedisCluster.hget("user:1", "name");
        System.out.println(value);
    }

    public static void main(String[] args) {
        ClusterNormalOperation normalOperation = new ClusterNormalOperation();
        normalOperation.saveData();
        normalOperation.getData();
        normalOperation.delData();
    }
}
