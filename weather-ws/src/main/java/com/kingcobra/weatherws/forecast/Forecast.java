package com.kingcobra.weatherws.forecast;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.weatherws.common.Constant;
import com.kingcobra.weatherws.exceptions.WeatherException;
import com.kingcobra.weatherws.services.BusinessRuleService;
import com.kingcobra.weatherws.services.RedisDataService;
import com.kingcobra.weatherws.utils.DateUtils;
import com.kingcobra.weatherws.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.JedisCluster;

import java.util.Date;

/**
 * Created by kingcobra on 15/11/28.
 */
@Controller
@RequestMapping("/forecast")
public class Forecast {
    private static final Logger LOGGER = LoggerFactory.getLogger(Forecast.class);

    @Autowired
    private RedisDataService redisDataService;

    @Autowired
    private BusinessRuleService businessRuleService;

    private static final RedisConnector redisConnector = RedisConnector.Builder.build();
    private static final JedisCluster jedisCluster = redisConnector.getJedisCluster();

    @RequestMapping(path="/{businessName}/{language}/{areaId}",method= RequestMethod.GET,produces = "application/json;charset=UTF-8")
    @ResponseBody
    public String handle(@PathVariable String businessName,@PathVariable String language,@PathVariable String areaId) {
        JSONObject forecastDatas= new JSONObject();
        Constant.Language lang = Constant.Language.valueOf(language.toUpperCase());

        //从redis缓存中取数据
        String currentDate = DateUtils.parseDate(new Date());
        char dayOrNight = DateUtils.calDayOrNight(currentDate);
        StringBuilder firstParam = new StringBuilder();
        firstParam.append(areaId + ".");
        firstParam.append(currentDate.substring(0, currentDate.length() - 2)+".");//去掉时间中的小时
        firstParam.append(dayOrNight);
        String cacheKey = String.format(Constant.CACHE_FORECAST_KEYTEMPLATE, firstParam.toString(), language);
        String cacheValue = jedisCluster.hget(cacheKey, businessName);

        if (StringUtils.isNotNullAndEmpty(cacheValue)) {
            return cacheValue;
        }else {
            try {
                JSONObject businessRule = businessRuleService.findBusinessRule(businessName);
                boolean isAround = businessRule.getBoolean(Constant.BUSINESSRULE_ISAROUNDAREA);
                boolean isAroundTravel = businessRule.getBoolean(Constant.BUSINESSRULE_ISAROUNDTRAVEL);
                if (!(isAround | isAroundTravel)) {
                    forecastDatas = redisDataService.singleAreaHandler(businessRule, areaId, lang);
                } else {
                    forecastDatas = redisDataService.aroundAreasHandler(businessRule, areaId, lang);
                }
                if (forecastDatas == null) {
                    forecastDatas = new JSONObject();
                    forecastDatas.put("status", Constant.ResponseStatus.FAILURE.toString());
                    forecastDatas.put("msg", "there have not forecast data");
                }else {
                    forecastDatas.put("status", Constant.ResponseStatus.SUCCESS.toString());
                    jedisCluster.hset(cacheKey, businessName, forecastDatas.toJSONString());
                    jedisCluster.expire(cacheKey, 24 * 3600);
                    LOGGER.info("data is {}",forecastDatas.toJSONString());
                    return forecastDatas.toJSONString();
                }
            } catch (WeatherException e) {
                LOGGER.error("failure message:{}", e.getMessage());
                forecastDatas.put("status", Constant.ResponseStatus.FAILURE.toString());
                forecastDatas.put("msg", e.getMessage());
            } catch (Exception e) {
                LOGGER.error("error message:", e);
                forecastDatas.put("status", Constant.ResponseStatus.ERROR.toString());
                forecastDatas.put("msg", "internal exception");
            }
            return forecastDatas.toJSONString();
        }
    }
}
