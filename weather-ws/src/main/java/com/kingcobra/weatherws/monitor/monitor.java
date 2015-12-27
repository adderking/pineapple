package com.kingcobra.weatherws.monitor;

import com.alibaba.fastjson.JSONArray;
import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.weatherws.common.Constant;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.JedisCluster;

import java.util.List;

/**
 * Created by kingcobra on 15/12/25.
 */
@Controller
@RequestMapping("/monitor")
public class monitor {

    private static final RedisConnector redisConnector = RedisConnector.Builder.build();
    private static final JedisCluster jedisCluster = redisConnector.getJedisCluster();

    @RequestMapping(value = "/stations/{dataType}/{startRow}",method= RequestMethod.GET,produces = "application/json;charset=utf-8")
    @ResponseBody
    public String stationMonitor(@PathVariable String dataType,@PathVariable int startRow,@RequestParam("paginate") int paginate) {
        String key = String.format(Constant.MONITORTABLENAME, dataType);
        List<String> monitorResult = jedisCluster.lrange(key,startRow,paginate);
        String responseData = JSONArray.toJSONString(monitorResult);
        return responseData;
    }

    @RequestMapping(value="/etl/{dataStructureName}/{startRow}",method=RequestMethod.GET,produces = "application/json;charset=utf-8")
    @ResponseBody
    public String etlMonitor(@PathVariable String dataStructureName,@PathVariable int startRow,@RequestParam("paginate") int paginate) {
        String key_3h = String.format(Constant.MONITORETLTABLENAME, dataStructureName);
        List<String> monitorData = jedisCluster.lrange(key_3h,startRow,paginate);
        String responseData = JSONArray.toJSONString(monitorData);
        return responseData;
    }
}
