package com.kingcobra.weatherws.dict;

import com.alibaba.fastjson.JSONArray;
import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.weatherws.common.Constant;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import redis.clients.jedis.JedisCluster;

import java.util.Map;

/**
 * Created by kingcobra on 15/12/28.
 */
@Controller
@RequestMapping("/dict")
public class Dicts {
    private RedisConnector redisConnector = RedisConnector.Builder.build();
    private JedisCluster jedisCluster = redisConnector.getJedisCluster();
    @RequestMapping(value = "/stations/{areaType}/{areaId}",method = RequestMethod.GET,produces = "application/json;charset=utf-8")
    @ResponseBody
    public String getStationData(@PathVariable String areaType,@PathVariable String areaId) {
        String keyTemplate = null;
        Constant.AreaType _areaType = Constant.AreaType.valueOf(areaType);
        switch (_areaType) {
            case internal:
                keyTemplate = Constant.AREA_TABLE;break;
            case external:
                keyTemplate = Constant.EXTERNAL_AREA_TABLE;
                break;
            case travel:
                keyTemplate = Constant.INTERNAL_TRAVEL_TABLE;
                break;
        }
        String key = String.format(keyTemplate, areaId);
        Map<String,String> data = jedisCluster.hgetAll(key);
        String jsonArray = JSONArray.toJSONString(data);
        return jsonArray;
    }
}
