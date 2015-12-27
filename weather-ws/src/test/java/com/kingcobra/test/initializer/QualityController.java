package com.kingcobra.test.initializer;

import com.kingcobra.kedis.core.RedisConnector;
import redis.clients.jedis.JedisCluster;

import java.util.Map;

/**
 * Created by kingcobra on 15/12/21.
 */
public class QualityController {
    private static final String qualityTable = "etl:weatherElement";
    private static final RedisConnector connector = RedisConnector.Builder.build();
    private JedisCluster jedisCluster = connector.getJedisCluster();

    public void getData() {
        Map<String, String> data = jedisCluster.hgetAll(qualityTable);
        for (Map.Entry<String, String> entry : data.entrySet()) {
            System.out.println(entry.getKey() + "," + entry.getValue());
        }
    }

    public static void main(String[] args) {
        QualityController qualityController = new QualityController();
        qualityController.getData();
    }
}
