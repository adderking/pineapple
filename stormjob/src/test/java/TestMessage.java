import com.kingcobra.kedis.core.RedisConnector;
import org.junit.Test;
import redis.clients.jedis.JedisCluster;

import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * Created by kingcobra on 15/10/13.
 */

public class TestMessage {
    @Test
    public void testMessage() {
        String seperator = ".";
        String[] column = {"stationId.code", "stationId"};
        for (String c : column) {
            String[] cc = c.split("\\.");
            System.out.println(cc.length);
        }
    }

    @Test
    public void testETL() {
        JedisCluster jedisCluster = RedisConnector.Builder.build().getJedisCluster();
        List<String> result = jedisCluster.lrange("monitor:etl:pmsc", 0, -1);
        for (String s : result) {
            System.out.println(s);
        }
    }
    @Test
    public void clearETLData() {
        String key = "monitor:etl:pmsc";
        JedisCluster jedisCluster = RedisConnector.Builder.build().getJedisCluster();
        Long expire = jedisCluster.expire(key,120);
        Long count =jedisCluster.llen(key);
        jedisCluster.del(key);
        System.out.println(expire);
        System.out.println(count);
    }
}
