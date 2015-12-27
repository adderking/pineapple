package com.kingcobra.test.getTestData;

import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import redis.clients.jedis.JedisCluster;

import javax.management.monitor.Monitor;
import java.util.Calendar;
import java.util.List;

/**
 * Created by kingcobra on 15/12/21.
 */
public class MonitorData {
    private static final String monitorTableName = "monitor:result:%s";
    private static final String monitorEtlTableName = "monitor:etl:%s";
    private static final RedisConnector connector = RedisConnector.Builder.build();
    private JedisCluster jedisCluster = connector.getJedisCluster();

    public void getStationMonitor() {
        String dataType = "pmsc_3h",dataType_12h="pmsc_12h";
        String key_3h = String.format(monitorTableName, dataType);
        List<String> monitorData = jedisCluster.lrange(key_3h, 0, -1);
        for (String s : monitorData) {
            JSONObject d = JSONObject.parseObject(s);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(d.getTimestamp("timestamp"));
            System.out.println(calendar.getTime()+":"+d.getJSONArray("missStations").toJSONString());
        }
    }

    public void getEltResult() {
        String dataType = "pmsc_fine_3h",dataType_12h="pmsc_12h";
        String key_3h = String.format(monitorEtlTableName, dataType);
        List<String> monitorData = jedisCluster.lrange(key_3h, 0, 2);
        System.out.println(monitorData);
        for (String s : monitorData) {
            System.out.println(s);
        }
    }
    public static void main(String[] args) {
        MonitorData monitorData = new MonitorData();
        monitorData.getStationMonitor();
        monitorData.getEltResult();
    }
}
