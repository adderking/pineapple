package com.kingcobra.weather;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.kingcobra.kedis.core.RedisConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.Map;

/**
 * Created by kingcobra on 15/10/11.
 */
public class WriteRedisBolt implements IRichBolt {
    private OutputCollector outputCollector;
    private TopologyContext context;
    private RedisConnector redisConnector;
    private JedisCluster jedisCluster;

    private final String recordIdentifier;//the single identifier of every single weather data
    private final String tableName;   //target table name
    private final String key;     //redis key column
    private static final String KEY_SEPERATOR = ".";
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteRedisBolt.class);
    /**
     *
     * @param tableName 保存到redis中的目标表名称
     * @param key   保存到redis中的数据的key，一般用stationId
     * @param recordIdentifier  一个站点不同时段的数据行的唯一标示符，可以使用vti或者预报时间。
     */
    public WriteRedisBolt(String tableName ,String key ,String recordIdentifier) {
        this.tableName = tableName;
        this.key = key;
        this.recordIdentifier = recordIdentifier;

    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.context = topologyContext;
        this.outputCollector = outputCollector;
        redisConnector = RedisConnector.Builder.build();
        jedisCluster = redisConnector.getJedisCluster();
    }

    /**
     * tuple is JSON structure data.
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        LOGGER.debug("table is {},key is {},recordIdentifier is {}", new String[]{tableName, key, recordIdentifier});
        if (Strings.isNullOrEmpty(tableName) || Strings.isNullOrEmpty(key) || Strings.isNullOrEmpty(recordIdentifier)) {
            this.outputCollector.fail(tuple);
        }
        JSONObject data = (JSONObject) tuple.getValueByField("data");
        String[] keyColumnName = key.split("\\.");
        String[] keyValue = new String[keyColumnName.length];
        String cName = null;
        for (int i = 0; i < keyColumnName.length; i++) {
            cName = keyColumnName[i];
            keyValue[i] = data.getString(cName);
        }
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(tableName + ":");
        int l = keyValue.length;
        for (int i = 0; i < l; i++) {
            keyBuilder.append(keyValue[i]);
            if (i != l - 1) {
                keyBuilder.append(KEY_SEPERATOR);
            }
        }
        String keyFieldName = data.getString(recordIdentifier);
        LOGGER.debug("data redis key is {}", keyBuilder.toString());
        LOGGER.debug("data fieldName is {},value is {}", keyFieldName, data.toJSONString());
        jedisCluster.hset(keyBuilder.toString(), keyFieldName, data.toJSONString());
//        jedisCluster.expire(keyBuilder.toString(), 120);
    }

    @Override
    public void cleanup() {
        redisConnector.closeJedisCluster();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
