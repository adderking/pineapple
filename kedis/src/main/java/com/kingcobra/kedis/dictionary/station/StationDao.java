package com.kingcobra.kedis.dictionary.station;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.DBConstant;
import com.kingcobra.kedis.core.RedisConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by kingcobra on 15/8/14.
 * dict_station table DAO
 */
public class StationDao {
    private static final JedisCluster jedisCluster = RedisConnector.getJedisCluster();
    private static final Logger LOGGER = LoggerFactory.getLogger(StationDao.class);

    /**
     * save area information
     * @param jsonObject
     * @return  -1: error ; 0 no operated ; 1 success
     */
    public long saveArea(JSONObject jsonObject) {
        String areaId = jsonObject.getString("areaId");
        if(areaId==null) {
            return -1;
        }
        String r_station_key = String.format(DBConstant.DICT_STATION_KEY, areaId);

        Set<String> keySet = jsonObject.keySet();
        Map<String, String> m_areaInfo = new HashMap<String, String>();
        for (String key : keySet) {
            if(!key.equalsIgnoreCase("areaAround")) {
                m_areaInfo.put(key, jsonObject.getString(key));
            }else {
                saveAreaAround(areaId, jsonObject.getJSONArray(key));
            }
        }
        String result = jedisCluster.hmset(r_station_key, m_areaInfo);
        saveIndex(areaId);
        return 1;
    }

    /**
     * get Area information by areaId
     * @param areaId
     * @return json object
     */
    public JSONObject getArea(String areaId) {
        String r_station_key = String.format(DBConstant.DICT_STATION_KEY, areaId);
        Map<String,String> m_areaInfo = jedisCluster.hgetAll(r_station_key);
        JSONObject result = (JSONObject)JSONObject.toJSON(m_areaInfo);
        LOGGER.info(result.toString());
        return result;
    }

    /**
     * get around area's information by areaId
     * @param areaId
     * @return json Array
     */
    public JSONArray getAreaAround(String areaId) {
        String r_stationAround_key = String.format(DBConstant.DICT_STATIONAROUND_KEY, areaId);
        Set<String> areaAround = jedisCluster.zrange(r_stationAround_key, 0l, -1l);
        JSONArray areas = new JSONArray();
        for (String s : areaAround) {
            areas.add(JSONObject.parseObject(s));
        }
        List<String> l_index = getIndex(1,2);
        for (String s : l_index) {
            LOGGER.info(s);
        }
        return areas;
    }

    private List<String> getIndex(long startIndex,long endIndex) {
        List<String> areaIndex = jedisCluster.lrange(DBConstant.STATION_INDEX_KEY, startIndex,endIndex);

        return areaIndex;
    }
    /**
     * save area index
     * @param areaId
     * @return
     */
    private long saveIndex(String areaId) {
        String r_stationIndex_key = DBConstant.STATION_INDEX_KEY;
        long result = jedisCluster.lpush(r_stationIndex_key, areaId);
        return result;
    }

    /**
     * save area around
     * @param jsonArray
     * @return
     */
    private long saveAreaAround(String areaId,JSONArray jsonArray) {
        if(jsonArray==null)
            return 0;
        Map<String, Double> m_areaAround = new HashMap<String, Double>();
        JSONObject element ;
        String r_key = String.format(DBConstant.DICT_STATIONAROUND_KEY, areaId);
        for (int i = 0; i < jsonArray.size(); i++) {
            element = jsonArray.getJSONObject(i);
            m_areaAround.put(element.toJSONString(), element.getDouble("score"));
        }
        long result = jedisCluster.zadd(r_key, m_areaAround);
        LOGGER.info(result + "");
        return 1;
    }


}
