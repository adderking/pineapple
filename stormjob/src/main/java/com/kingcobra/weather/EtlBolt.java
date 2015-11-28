package com.kingcobra.weather;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.kingcobra.kedis.core.RedisConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kingcobra on 15/10/11.
 */
public class EtlBolt implements IRichBolt {
    private OutputCollector outputCollector;
    private TopologyContext topologyContext;
    private JSONArray columns;
    private static final String WEATHER_ELEMENTELT_POLICY_KEY = "etl:weatherElement";
    private Map<String, String> eltPolicy = new HashMap<String, String>();
    private RedisConnector redisConnector;
    private String dataStructureName;
    private String monitor_key;
    private static final Logger LOGGER = LoggerFactory.getLogger(EtlBolt.class);
    public EtlBolt(String dataStructureName,JSONArray columns) {
        this.dataStructureName = dataStructureName;
        this.columns = columns;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
        redisConnector =RedisConnector.Builder.build();
        JedisCluster jedisCluster = redisConnector.getJedisCluster();
        eltPolicy = jedisCluster.hgetAll(WEATHER_ELEMENTELT_POLICY_KEY);
        monitor_key = String.format(Constant.MONITOR_ETL, dataStructureName);
    }

    /**
     * 将tuple中包含的数据按照columns变量格式封装成JSON对象，columns包含的内容作为属性名，tuple中的数据作为属性值。
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        byte[] msg = tuple.getBinary(0);
        String content = new String(msg, Charsets.UTF_8);
        JSONObject newData = makeJSONData(content);
        this.outputCollector.emit(tuple,new Values(newData));

        this.outputCollector.ack(tuple);

        LOGGER.debug("orginal data is {}", content);
        LOGGER.debug("json data is {}", newData.toJSONString());
    }

    /**
     * 将tuple中包含的数据转换成JSON对象
     *
     * @param content
     * @return
     */
    private JSONObject makeJSONData(String content) {
        JSONObject record = new JSONObject();
        JSONObject elementEtlPolicy= null;
        String[] _weatherData = content.split("\\,");
        String field = null, src_value = null,dst_value;
        HashMap<String, JSONObject> etlMonitor = new HashMap<String, JSONObject>();
        JedisCluster jedisCluster = redisConnector.getJedisCluster();
        for (int i = 0; i < columns.size(); i++) {
            field = columns.getString(i);
            if ("$t".equalsIgnoreCase(field)) {
                continue;
            } else {
                dst_value = src_value = _weatherData[i];
                if (eltPolicy.containsKey(field)) {
                    elementEtlPolicy = JSONObject.parseObject(eltPolicy.get(field));
                    dst_value = tranformValue(src_value, elementEtlPolicy);
                    if(!dst_value.equalsIgnoreCase(src_value)) {
                        jedisCluster.lpush(monitor_key, content);
                    }
                }
                record.put(field,dst_value);
            }
        }
        return record;
    }

    /**
     * 根据气象元素值的清洗规则进行清洗
     * @param v 原值
     * @param etlRule   清洗规则
     * @return  清洗后的值
     */
    private String tranformValue(String v, JSONObject etlRule) {
        Double _v;
        Double minValue = etlRule.getDoubleValue(Constant.MIN);
        Double maxValue = etlRule.getDoubleValue(Constant.MAX);
        Double defaultValue = etlRule.getDoubleValue(Constant.DEFAULT);
        if (Strings.isNullOrEmpty(v)) {
            _v = defaultValue;
            return _v.toString();
        }
        _v = Double.valueOf(v);
        if (_v.doubleValue() < minValue.doubleValue()) {
            _v = minValue.doubleValue();
            return _v.toString();
        }
        if (_v.doubleValue() > maxValue.doubleValue()) {
            _v = maxValue.doubleValue();
            return _v.toString();
        }
        return v;
    }
    @Override
    public void cleanup() {
        redisConnector.closeJedisCluster();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public class Constant {
        public static final String MAX = "max";
        public static final String MIN = "min";
        public static final String DEFAULT = "default";
        public static final String MONITOR_ETL = "monitor:etl:%s";
    }
}
