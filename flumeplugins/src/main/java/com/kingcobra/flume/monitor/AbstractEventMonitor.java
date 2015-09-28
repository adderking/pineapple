package com.kingcobra.flume.monitor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.flume.util.StringBloomFilter;
import com.kingcobra.kedis.core.RedisConnector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Set;

/**
 * Created by kingcobra on 15/9/22.
 */
public abstract class AbstractEventMonitor implements EventMonitor {
    private RedisConnector redisConnector;
    private String dataType;  //数据类型，作为redis key保存比较结果。
    private String stationsTable; //该数据类型对应的全部站点ID集合
    private final String JEDIS_KEY = "monitor:%s";
    private String redisNodes;
    private StringBloomFilter stringBloomFilter;
    private EventParser parseEvent;

    public AbstractEventMonitor(Context ctx) {
        this.dataType = ctx.getString(Constant.MONITOR_DATATYPE);
        this.stationsTable = ctx.getString(Constant.MONITOR_STATIONS);
        this.redisNodes = ctx.getString(Constant.MONITOR_REDIS);
    }

    public AbstractEventMonitor(String dataType,String stationsTable,String redisNodes) {
        this.dataType = dataType;
        this.stationsTable = stationsTable;
        this.redisNodes = redisNodes;
    }
    /**
     * 保存每个event中的stationId到bloomFilter中
     * @param event
     */
    @Override
    public void readEvent(Event event) {
        String stationId = this.parseEvent.parseEvent(event);
        stringBloomFilter.put(stationId);
    }

    @Override
    public void stationsDiff() {
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
        redisConnector = RedisConnector.Builder.build(redisNodes, Constant.REDIS_DEFAULT_MAXTOTAL,Constant.REDIS_DEFAULT_MAXIDEL,Constant.REDIS_DEFAULT_MAXWAITMILLIS);
        this.parseEvent  = parseEvent;
    }

    @Override
    public void readEvents(List<org.apache.flume.Event> events) {
        if(events.isEmpty())
            return;
        for (org.apache.flume.Event event : events) {
            readEvent(event);
        }
    }
    public void close() {
        stringBloomFilter = null;
        if(redisConnector!=null)
            redisConnector.closeJedisCluster();
    }
    @Override
    public void refreshBloomFilter() {
        stringBloomFilter= new StringBloomFilter(Constant.BLOOMFILTER_EXPECTINSERTIONS,Constant.BLOOMFILTER_POSITIVE);
    }

    public static abstract class EventParser {
        private Context ctx;
        public void setContext(Context ctx){
            this.ctx = ctx;
        }
        public abstract String parseEvent(Event event);
    }
}
