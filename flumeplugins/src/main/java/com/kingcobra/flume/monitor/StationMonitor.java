package com.kingcobra.flume.monitor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.kingcobra.flume.util.StringBloomFilter;
import com.kingcobra.kedis.core.RedisConnector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Set;


/**
 * Created by kingcobra on 15/9/20.
 */
public class StationMonitor extends AbstractEventMonitor{

    private static final Logger LOGGER = LoggerFactory.getLogger(StationMonitor.class);

    private String dataType;  //数据类型，作为redis key保存比较结果。
    private String stationsTable; //该数据类型对应的全部站点ID集合
    private final String JEDIS_KEY = "monitor:%s";
    private StringBloomFilter stringBloomFilter;

    public StationMonitor(Context ctx) {
        super(ctx.getString(Constant.MONITOR_REDIS));
        this.dataType = ctx.getString(Constant.MONITOR_DATATYPE);
        this.stationsTable = ctx.getString(Constant.MONITOR_STATIONS);
    }

    public StationMonitor(String dataType,String stationsTable,String redisNodes) {
        super(redisNodes);
        this.dataType = dataType;
        this.stationsTable = stationsTable;
    }
    /**
     * 保存每个event中的stationId到bloomFilter中
     * @param event
     */
    @Override
    public void eventMonitored(Event event) {
        String stationId = parseEvent.parseEvent(event);
        if(!Strings.isNullOrEmpty(stationId))
            stringBloomFilter.put(stationId);
    }

    /**
     * 计算BloomFilter和redis中全部站点的差集
     */
    @Override
    public void monitorResult() {
        JedisCluster jedisCluster = redisConnector.getJedisCluster();
        // 计算两个集合差值，得到本次缺失的区域。
        JSONArray missStations = new JSONArray();
        Set<String> totalStations = jedisCluster.smembers(stationsTable);
        for (String stationId : totalStations) {
            if (!stringBloomFilter.isContain(stationId)) {
                missStations.add(stationId);
            }
        }
        JSONObject result = new JSONObject();
        result.put("timestamp", System.currentTimeMillis());
        result.put("missStations", missStations);
        String monitorResultTable = String.format(JEDIS_KEY, dataType);
        jedisCluster.lpush(monitorResultTable, result.toJSONString());
    }

    @Override
    public void initialize(EventParser parseEvent) {
        stringBloomFilter= new StringBloomFilter(Constant.BLOOMFILTER_EXPECTINSERTIONS,Constant.BLOOMFILTER_POSITIVE);
        super.initialize(parseEvent);
    }

    @Override
    public void close() {
        stringBloomFilter = null;
        super.close();
    }

    /**
     * 重置bloomFilter
     */
    @Override
    public void resetMonitor() {
        stringBloomFilter= new StringBloomFilter(Constant.BLOOMFILTER_EXPECTINSERTIONS,Constant.BLOOMFILTER_POSITIVE);
    }

}
