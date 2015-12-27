import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.kedis.util.JedisUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.Set;

/**
 * Created by kingcobra on 15/12/12.
 */
public class TestJedisPipelineAndLua {

    private RedisConnector redisConnector = RedisConnector.Builder.build();
    private JedisCluster jedisCluster = redisConnector.getJedisCluster();
    private Client redisClient = new Client("192.168.1.112",6378);
    private JedisPool jedisPool = redisConnector.getJedisPool("192.168.1.114", 6379);
    private static final String KEY_FORMAT = "testKey:%s";
    /*@Before
    public void initData() {
        for (int i = 0; i < 10000; i++) {
            String key = String.format(KEY_FORMAT, i);
            jedisCluster.set(key, "value" + i);
            jedisCluster.expire(key, 3600);
        }
    }*/

    @Test
    public void testGetKey() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("local result = {}");
        stringBuffer.append("result = redis.call('get',KEYS)");
        stringBuffer.append("return result");

        String[] params = new String[5];
        for (int i = 0; i < 5; i++) {
            String key = String.format(KEY_FORMAT, i);
            params[i] = key;
        }
        Jedis jedis = jedisPool.getResource();
        String s = jedisCluster.get(params[0]);
        Map<String, JedisPool> jedisPools = jedisCluster.getClusterNodes();
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            System.out.println(entry.getKey());
        }
        /*Object object = jedis.eval(stringBuffer.toString(),1, params[1] );
        System.out.println(object);*/
    }
}
