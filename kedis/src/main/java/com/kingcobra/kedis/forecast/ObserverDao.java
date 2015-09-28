package com.kingcobra.kedis.forecast;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.DBConstant;
import com.kingcobra.kedis.core.RedisConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by kingcobra on 15/9/6.
 */
public class ObserverDao {
    private static RedisConnector redisCluster = RedisConnector.Builder.build();
    private static Logger LOGGER = LoggerFactory.getLogger(ObserverDao.class);

    /**
     * key :observe:%stationId:%c_bjtime
     * @param jsonObject structure is {stationId:,c_bjtime:,temp:,wd:,ws:,wet:,rain1h:,rain6h:,rain12h:}
     * @return -1: error ; 0 no operated ; 1 success
     */
    public long saveObserver(JSONObject jsonObject) {
        JedisCluster jedisCluster = redisCluster.getJedisCluster();
        String stationId = jsonObject.getString("stationId");
        String c_bjtime = jsonObject.getString("c_bjtime");
        if (stationId == null || c_bjtime==null) {
            return -1;
        }
        String observeKey = String.format(DBConstant.OBSERVE_KEY, stationId, c_bjtime);
        Set<String> keySet = jsonObject.keySet();
        Map<String, String> m_areaInfo = new HashMap<String, String>();
        for (String key : keySet) {
            m_areaInfo.put(key, jsonObject.getString(key));
        }
        String result = jedisCluster.hmset(observeKey, m_areaInfo);
        return 1;
    }

    public JSONObject getObserve(String key) {
        JedisCluster jedisCluster = redisCluster.getJedisCluster();
        Map<String, String> observe = jedisCluster.hgetAll(key);
        JSONObject result = (JSONObject)JSONObject.toJSON(observe);
        LOGGER.info(result.toString());
        return result;
    }

    public static void main(String[] args) {
      String data1 = "{stationId:\"54511\",c_bjtime:\"20150731120000\",temp:\"28.3\",wd:\"0\",ws:\"2\",wet:\"99\"}";
        String data2 = "{stationId:\"54511\",c_bjtime:\"20150731120001\",temp:\"7.4\",wd:\"1\",ws:\"1\",wet:\"10\"}";
        String data3 = "{stationId:\"54511\",c_bjtime:\"20150731120002\",temp:\"7.4\",wd:\"1\",ws:\"1\",wet:\"10\"}";
        JSONObject object = JSON.parseObject(data1);
        JSONObject object2 = JSON.parseObject(data2);
        JSONObject object3 = JSON.parseObject(data3);
        ObserverDao dao = new ObserverDao();
        dao.saveObserver(object);
        dao.saveObserver(object2);
        dao.saveObserver(object3);
        String key = "observe:54511:20150731120001";
        dao.getObserve(key);
        key = "observe:54511:20150731120000";
        dao.getObserve(key);
        key = "observe:54511:20150731120002";
        dao.getObserve(key);

    }
}
