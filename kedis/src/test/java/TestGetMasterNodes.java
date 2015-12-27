import com.kingcobra.kedis.core.RedisConnector;
import junit.framework.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/9/7.
 */
public class TestGetMasterNodes {
    public void getNodes() {
        RedisConnector redisConnector = RedisConnector.Builder.build();
        JedisPool jedisPool = redisConnector.getJedisPool("192.168.1.112", 6379);
        Jedis jedis = jedisPool.getResource();
        String info = jedis.clusterInfo();
        System.out.println(info);
        String nodes = jedis.clusterNodes();
        String s = "[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}:[0-9]{4} master";
        Pattern pattern = Pattern.compile(s);
        Matcher matcher = pattern.matcher(nodes);
        while(matcher.find()) {
            System.out.println(nodes.substring(matcher.start(), matcher.end()));
        }
    }

    public static void main(String[] args) {
        TestGetMasterNodes nodes = new TestGetMasterNodes();
        nodes.getNodes();

    }
}
