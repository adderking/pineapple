package com.kingcobra.weatherws.services;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.weatherws.common.BusinessRuleHelper;
import com.kingcobra.weatherws.common.Constant;
import com.kingcobra.weatherws.common.DictHelper;
import com.kingcobra.weatherws.utils.DateUtils;
import com.kingcobra.weatherws.utils.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisCluster;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by kingcobra on 15/12/1.
 */
@Service("dataService")
public class RedisDataService {

    private final RedisConnector redisConnector;
    private final JedisCluster jedisCluster;

    @Autowired
    private DictHelper dictHelper;
    @Autowired
    private BusinessRuleHelper businessRuleHelper;
    public RedisDataService() {
        redisConnector = RedisConnector.Builder.build();
        jedisCluster = redisConnector.getJedisCluster();
    }

    /**
     * 获得areaId的天气预报
     * @param businessRule 业务规则
     * @param areaId    区域ID
     * @return JSONObject 格式：{‘forecast':{'areaInfo':,’data':[]}}
     */
    public JSONObject singleAreaHandler(JSONObject businessRule,String areaId,Constant.Language language) {
        String timeScope[] = businessRuleHelper.getTimeRange(businessRule);
        String tableName = businessRule.getString(Constant.BUSINESSRULE_DATATABLENAME);
        String columns = businessRule.getString(Constant.BUSINESSRULE_COLUMNS);
        JSONArray columnArray = JSONArray.parseArray(columns);
        String timeColumn = businessRule.getString(Constant.BUSINESSRULE_TIMECOLUMN);
        JSONObject areaInfo = businessRuleHelper.getAreaInfo(businessRule, areaId, language);
        String stationId = areaInfo.getString(Constant.STATIONID);
        String key = tableName + ":" + stationId;
        JSONArray forecastData = findForecastWithKey(key, timeScope, timeColumn, columnArray,language);
        JSONObject singleAreaForecast = new JSONObject();
        singleAreaForecast.put("areaInfo", areaInfo);
        singleAreaForecast.put("data", forecastData);
        JSONObject resultData = new JSONObject();
        if (forecastData == null || forecastData.size() == 0) {
            resultData = null;
        }else {
            resultData.put("forecast", singleAreaForecast);
        }
        return resultData;
    }

    /**
     * 判断预报数据的时间是否在取值的时间范围内
     * @param timeValue 预报时间
     * @param timeScope 时间范围
     * @return 如果在范围内，返回true，否则返回false;
     */
    private boolean compareTime(String timeValue, String[] timeScope) {
        long time = Long.valueOf(timeValue);
        long startTime = Long.valueOf(timeScope[0]);
        long endTime = Long.valueOf(timeScope[1]);
        if (time < endTime & time >= startTime) {
            return true;
        }
        return false;
    }
    /**
     * 通过redis key获得预报数据
     * @param key
     * @return 排序好的数据
     */
    private JSONArray findForecastWithKey(String key,String[] timeScope,String timeColumn,JSONArray columns,Constant.Language language) {
        Map<String, String> forecastData = jedisCluster.hgetAll(key);
        Map<String, String> forecastDataOrdered = new TreeMap<String, String>(new DataComparator());
        forecastDataOrdered.putAll(forecastData);
        JSONArray forecasts = new JSONArray();
        for (Map.Entry<String, String> entry : forecastDataOrdered.entrySet()) {
            String forecastDetail = entry.getValue();
            JSONObject o_data = JSONObject.parseObject(forecastDetail);
            String timeValue = o_data.getString(timeColumn);
            boolean inScope = compareTime(timeValue, timeScope);
            if(!inScope)
                continue;
            JSONObject result = new JSONObject();
            char dayOrNight = DateUtils.calDayOrNight(timeValue);
            switch(dayOrNight){
                case Constant.WEATHER_DAY:
                    result.put(Constant.WEATHER_TIME, "白天");break;
                case Constant.WEATHER_NIGHT:
                    result.put(Constant.WEATHER_TIME,"夜间");break;
                default:
                    result.put(Constant.WEATHER_TIME, "白天");break;
            }
            for (int j = 0; j < columns.size(); j++) {
                String columnName = columns.getString(j);
                String value = o_data.getString(columnName);
                if (Constant.WEATHER_LEVEL.equalsIgnoreCase(columnName)) {
                    String formattedValue = StringUtils.formatNumeric(value,"00");
                    result.put(Constant.WEATHER_CODE, dayOrNight +formattedValue);
                    String weatherDesc = dictHelper.weatherLevelParser(formattedValue, language);
                    result.put(Constant.WEATHER_DESC,weatherDesc);
                }else if (Constant.FF_LEVEL.equalsIgnoreCase(columnName)) {
                    String formattedValue = StringUtils.formatNumeric(value,"00");
                    String ffDesc = dictHelper.ffLevelParser(formattedValue,language);
                    result.put(Constant.FF_DESC, ffDesc);
                }else if (Constant.DD_LEVEL.equalsIgnoreCase(columnName)) {
                    String formatterValue = StringUtils.formatNumeric(value,"00");
                    String ddDesc = dictHelper.ddLevelParser(formatterValue,language);
                    result.put(Constant.DD_DESC, ddDesc);
                }else {
                    result.put(columnName, value);
                }
            }
            forecasts.add(result);
        }
        return forecasts;
    }


    /**
     * 获得areaId的周边区域的天气预报
     * @param businessRule
     * @param areaId
     * @return JSONObject 格式：{‘forecast':[{'areaId':,'stationId':,'name':,'data':[]},...]}
     */
    public JSONObject aroundAreasHandler(JSONObject businessRule, String areaId,Constant.Language language) {
        JSONArray aroundAreas = businessRuleHelper.getAroundArea(businessRule, areaId,language);
        JSONObject resultData = new JSONObject();
        if(aroundAreas==null || aroundAreas.size()==0)
             resultData =null;
        else {
            String startTime = "now";
            String endTime = "now+1";
            String timeScope[] = DateUtils.calTimeScope(startTime, endTime);
            String tableName = businessRule.getString(Constant.BUSINESSRULE_DATATABLENAME);
            JSONArray columns = businessRule.getJSONArray(Constant.BUSINESSRULE_COLUMNS);
            String timeColumn = businessRule.getString(Constant.BUSINESSRULE_TIMECOLUMN);
            String stationId, key;
            JSONObject areaInfo;
            for (int i = 0; i < aroundAreas.size(); i++) {
                areaInfo = aroundAreas.getJSONObject(i);
                stationId = areaInfo.getString(Constant.STATIONID);
                key = tableName + ":" + stationId;
                JSONArray forecasts = findForecastWithKey(key, timeScope, timeColumn, columns, language);
                areaInfo.put("data", forecasts);
            }
            resultData.put("forecast", aroundAreas);
        }
        return resultData;
    }
    /*
    为数据排序的比较器
     */
    private class DataComparator implements Comparator<String>{
        @Override
        public int compare(String o1, String o2) {
            int _o1 = Integer.valueOf(o1);
            int _o2 = Integer.valueOf(o2);
            return _o1 - _o2;
        }
    }

}
