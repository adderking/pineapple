package scistor.testjedis;

import com.kingcobra.kedis.core.RedisConnector;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by kingcobra on 15/8/13.
 */
public class IndexmeaningOperation {
    private JedisCluster jedisCluster = RedisConnector.getJedisCluster();
    private static final String PREFIX_KEY = "dict:indexmeaning:";
    /*
        hash type
     */
    public long save() {
        long result = jedisCluster.hset(PREFIX_KEY+"101010100", "stationId", "101010100");
        result = jedisCluster.hset(PREFIX_KEY + "101010100", "areaId", "10101");
        System.out.println(result);
        return result;
    }

    public void get(String stationId) {
        Map<String,String> v =  jedisCluster.hgetAll(PREFIX_KEY + stationId);//返回JSON类型
        System.out.println(v);
        List<String> values = jedisCluster.hvals(PREFIX_KEY + stationId);//返回所有value
        for (String s : values) {
            System.out.println(s);
        }
    }

    /*
        Sorted Set type
     */
    public void sortedSet() {
        jedisCluster.zadd("dict:stationAround:101010100", 100, new String("[{aroundStation:1,aroundArea:1},{aroundStation:2,aroundArea:2}]"));
        jedisCluster.zadd("dict:stationAround:101010100", 90, new String("[{aroundStation:3,aroundArea:3},{aroundStation:4,aroundArea:4}]"));
    }

    public void getByScore() {
        Set<String> values = jedisCluster.zrangeByScore("dict:stationAround:101010100", 90d, 100d);
        for (String s : values) {
            System.out.println(s);
        }
    }

    /*
        String type
     */
    public void testStringType() {
        System.out.println(jedisCluster.get("c"));
    }
    public static void main(String[] args) {
        IndexmeaningOperation indexmeaningOperation = new IndexmeaningOperation();
        /*indexmeaningOperation.save();
        indexmeaningOperation.get("101010100");*/
        indexmeaningOperation.sortedSet();
        indexmeaningOperation.getByScore();
        indexmeaningOperation.testStringType();
    }
}
