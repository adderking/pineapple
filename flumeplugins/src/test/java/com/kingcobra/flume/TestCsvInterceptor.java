package com.kingcobra.flume;

import com.kingcobra.kedis.core.RedisConnector;
import com.sun.org.apache.xalan.internal.xsltc.dom.FilteredStepIterator;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.*;
import org.junit.Test;
import redis.clients.jedis.JedisCluster;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by kingcobra on 15/9/17.
 */
public class TestCsvInterceptor {
    private Set<String> data =new HashSet<String>();
    RedisConnector redisConnector;
//    @Before
    public void initial() {
        data.add("3773,20150731000000,20150731000000,0,20150730200000,20150731080000,201507302100000000,0,14");
        data.add(null);

        JedisCluster jedisCluster = redisConnector.getJedisCluster();
        String key = "monitor:external.stations";
        for (int i = 1000; i < 9999; i++) {
            jedisCluster.sadd(key, String.valueOf(i));
        }
        redisConnector.closeJedisCluster();

    }
//    @Test
    public void testBasic() throws ClassNotFoundException,InstantiationException,IllegalAccessException {
        Context ctx = new Context();
        ctx.put("monitor.key", "0");
        ctx.put("monitor.dataType","forecast3h.external");
        ctx.put("monitor.stations", "monitor:external.stations");
        InterceptorBuilderFactory factory = new InterceptorBuilderFactory();

        Interceptor.Builder build = InterceptorBuilderFactory.newInstance("com.kingcobra.flume.interceptor.CsvInterceptor$Builder");
        build.configure(ctx);
        Interceptor interceptor = build.build();
        interceptor.initialize();
        Event event ;
        for (String s : data) {
            if (s == null)
                event = null;
            else
                event = EventBuilder.withBody(s, Charset.forName("UTF-8"));
            event = interceptor.intercept(event);
        }
    }
    @Test
    public void getRedisData() {
        String key = "monitor:forecast3h.external";
        JedisCluster jedisCluster = redisConnector.getJedisCluster();
        List<String> result = jedisCluster.lrange(key, 0, 10);
        for (String s : result) {
            System.out.println(s);
        }
        redisConnector.closeJedisCluster();
    }
}
