package com.kingcobra.weatherws.dict;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.weatherws.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import redis.clients.jedis.JedisCluster;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by kingcobra on 15/12/28.
 */
@Controller
@RequestMapping("/dict")
public class Dicts {
    private RedisConnector redisConnector = RedisConnector.Builder.build();
    private JedisCluster jedisCluster = redisConnector.getJedisCluster();
    private static final String persistRuleName = "persistPolicy:%s";
    private static final String persistTableName = "persistTarget:%s";
    private static final Logger log = LoggerFactory.getLogger(Dicts.class);
    @RequestMapping(value = "/areas/{areaType}/{areaId}",method = RequestMethod.GET,produces = "application/json;charset=utf-8")
    @ResponseBody
    public String getAreaData(@PathVariable String areaType,@PathVariable String areaId) {
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
    @RequestMapping(value = "/aroundareas/{areaType}/{areaId}",method = RequestMethod.GET,produces = "application/json;charset=utf-8")
    @ResponseBody
    public String getAroundAreaData(@PathVariable String areaType,@PathVariable String areaId) {
        String keyTemplate = null;
        Constant.AreaType _areaType = Constant.AreaType.valueOf(areaType);
        switch (_areaType) {
            case internal:
                keyTemplate = Constant.INTERNAL_AROUNDAREA_TABLE;break;
            case external:
                keyTemplate = Constant.EXTERNAL_AROUNDAREA_TABLE;
                break;
            case travel:
                keyTemplate = Constant.INTERNAL_AROUNDTRAVEL_TABLE;
                break;
        }
        String key = String.format(keyTemplate, areaId);
        Set<String> data = jedisCluster.zrange(key, 0, -1);
        String jsonArray = JSONArray.toJSONString(data);
        return jsonArray;
    }
    @RequestMapping(value = "/businessrule/{businessRuleName}",method = RequestMethod.GET,produces = "application/json;charset=utf-8")
    @ResponseBody
    public String getBusinessRule(@PathVariable String businessRuleName) {
        String key = String.format(Constant.BUSINESSRULE_TABLE, businessRuleName);
        Map<String,String> values = jedisCluster.hgetAll(key);
        JSONObject rule = (JSONObject)JSONObject.toJSON(values);
        return rule.toJSONString();
    }
    @RequestMapping(value = "/persistrule/{persistrule}",method = RequestMethod.GET,produces = "application/json;charset=utf-8")
    @ResponseBody
    public String getPersistRule(@PathVariable String persistrule) {
        String key = String.format(persistRuleName, persistrule);
        Map<String,String> values = jedisCluster.hgetAll(key);
        JSONObject rule = (JSONObject)JSONObject.toJSON(values);
        return rule.toJSONString();
    }
    @RequestMapping(value = "/persisttable/{topic}",method = RequestMethod.GET,produces = "application/json;charset=utf-8")
    @ResponseBody
    public String getPersistTable(@PathVariable String topic) {
        String key = String.format(persistTableName, topic);
        Map<String,String> values = jedisCluster.hgetAll(key);
        JSONObject rule = (JSONObject)JSONObject.toJSON(values);
        return rule.toJSONString();
    }
    @RequestMapping(value = "/initBusiness/{fileName}", method = RequestMethod.GET, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String initBusiness(@PathVariable String fileName) {
        String value = null;
        try {
            FileInputStream inputStream = new FileInputStream(new File("/home/datauser/init/" + fileName));
            byte[] buffer = new byte[1024];
            int i=0;
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            while ((i=inputStream.read(buffer))>=0) {
                byteArrayOutputStream.write(buffer,0,i);
            }
            byte[] content = byteArrayOutputStream.toByteArray();
            value= new String(content, "UTF-8");
            JSONObject _rule = JSONObject.parseObject(value);
            String businessRuleName = _rule.getString("RuleName");
            String key = String.format(Constant.BUSINESSRULE_TABLE, businessRuleName);
            Map<String, String> rule = new HashMap<String, String>();
            rule.put(Constant.BUSINESSRULE_DBTYPE, _rule.getString(Constant.BUSINESSRULE_DBTYPE));
            rule.put(Constant.BUSINESSRULE_DATATABLENAME, _rule.getString(Constant.BUSINESSRULE_DATATABLENAME));
            rule.put(Constant.BUSINESSRULE_TIMECOLUMN, _rule.getString(Constant.BUSINESSRULE_TIMECOLUMN));
            rule.put(Constant.BUSINESSRULE_COLUMNS, _rule.getString(Constant.BUSINESSRULE_COLUMNS));
            rule.put(Constant.BUSINESSRULE_ISAROUNDAREA, _rule.getString(Constant.BUSINESSRULE_ISAROUNDAREA));
            rule.put(Constant.BUSINESSRULE_ISAROUNDTRAVEL, _rule.getString(Constant.BUSINESSRULE_ISAROUNDTRAVEL));
            rule.put(Constant.BUSINESSRULE_ISWEEKEND, _rule.getString(Constant.BUSINESSRULE_ISWEEKEND));
            rule.put(Constant.BUSINESSRULE_AREATYPE, _rule.getString(Constant.BUSINESSRULE_AREATYPE));
            rule.put(Constant.BUSINESSRULE_STARTTIME, _rule.getString(Constant.BUSINESSRULE_STARTTIME));
            rule.put(Constant.BUSINESSRULE_ENDTIME, _rule.getString(Constant.BUSINESSRULE_ENDTIME));
            String result = jedisCluster.hmset(key, rule);
            inputStream.close();
            byteArrayOutputStream.close();
            value += "\n" + result;
        } catch (IOException exception) {
            log.error("error message:", exception);
            value = "read file error";
        }
        return value;
    }

    @RequestMapping(value = "/initPersistPolicy/{fileName}", method = RequestMethod.GET, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String initPersistPolicy(@PathVariable String fileName) {
        String value = null;
        try {
            FileInputStream inputStream = new FileInputStream(new File("/home/datauser/init/" + fileName));
            byte[] buffer = new byte[1024];
            int i=0;
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            while ((i=inputStream.read(buffer))>=0) {
                byteArrayOutputStream.write(buffer,0,i);
            }
            byte[] content = byteArrayOutputStream.toByteArray();
            value= new String(content, "UTF-8");
            JSONObject _rule = JSONObject.parseObject(value);
            String policyName = _rule.getString("policyName");
            String key = String.format(persistRuleName,policyName);
            Map<String,String> rule = new HashMap<String, String>();
            rule.put("columns", _rule.getJSONArray("columns").toJSONString());
            rule.put("type", _rule.getJSONArray("type").toJSONString());
            String result = jedisCluster.hmset(key, rule);
            inputStream.close();
            byteArrayOutputStream.close();
            value += "\n" + result;
        } catch (IOException exception) {
            log.error("error message:", exception);
            value = "read file error";
        }
        return value;
    }
    @RequestMapping(value = "/initPersistTable/{fileName}", method = RequestMethod.GET, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String initPersistTable(@PathVariable String fileName) {
        String value = null;
        try {
            FileInputStream inputStream = new FileInputStream(new File("/home/datauser/init/" + fileName));
            byte[] buffer = new byte[1024];
            int i=0;
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            while ((i=inputStream.read(buffer))>=0) {
                byteArrayOutputStream.write(buffer,0,i);
            }
            byte[] content = byteArrayOutputStream.toByteArray();
            value= new String(content, "UTF-8");
            JSONObject _rule = JSONObject.parseObject(value);
            String topic = _rule.getString("topic");
            String key = String.format(persistTableName, topic);
            Map<String,String> rule = new HashMap<String, String>();
            rule.put("redis", _rule.getString("redis"));
            rule.put("rule", _rule.getString("rule"));
            String result = jedisCluster.hmset(key, rule);
            inputStream.close();
            byteArrayOutputStream.close();
            value += "\n" + result;
        } catch (IOException exception) {
            log.error("error message:", exception);
            value = "read file error";
        }
        return value;
    }
    @RequestMapping(value = "/initetlrule", method = RequestMethod.GET, produces = "application/json;charset=utf-8")
    @ResponseBody
    public void persistWeatherElementEtlPolicy() {
        String redisKey = "etl:weatherElement";
        JSONObject eltPolicy = new JSONObject();
        //温度
        String elementName = "TEMP";
        eltPolicy.put("min", -50.0);
        eltPolicy.put("max", 50.0);
        eltPolicy.put("default", 0);
        jedisCluster.hset(redisKey, elementName, eltPolicy.toJSONString());
        //最小温度
        elementName = "MIN_TEMP";
        eltPolicy.put("min", -50.0);
        eltPolicy.put("max", 50.0);
        eltPolicy.put("default", 0);
        jedisCluster.hset(redisKey, elementName, eltPolicy.toJSONString());
        //最大温度
        elementName = "MAX_TEMP";
        eltPolicy.put("min", -50.0);
        eltPolicy.put("max", 50.0);
        eltPolicy.put("default", 0);
        jedisCluster.hset(redisKey, elementName, eltPolicy.toJSONString());
        //风速
        elementName = "FF";
        eltPolicy.put("min", 0.0);
        eltPolicy.put("max", 70.0);
        eltPolicy.put("default", 0);
        jedisCluster.hset(redisKey, elementName, eltPolicy.toJSONString());
        //降水
        elementName = "RAIN";
        eltPolicy.put("min", 0.0);
        eltPolicy.put("max", 2000.0);
        eltPolicy.put("default", 0);
        jedisCluster.hset(redisKey, elementName, eltPolicy.toJSONString());
        //风向
        elementName = "DD";
        eltPolicy.put("min", 0.0);
        eltPolicy.put("max", 360.0);
        eltPolicy.put("default", 0);
        jedisCluster.hset(redisKey, elementName, eltPolicy.toJSONString());
        //云量
        elementName = "CLOUD";
        eltPolicy.put("min", 0.0);
        eltPolicy.put("max", 100.0);
        eltPolicy.put("default", 0);
        jedisCluster.hset(redisKey, elementName, eltPolicy.toJSONString());
        //湿度
        elementName = "RH";
        eltPolicy.put("min", 0.0);
        eltPolicy.put("max", 100.0);
        eltPolicy.put("default", 0);
        jedisCluster.hset(redisKey, elementName, eltPolicy.toJSONString());
    }
    @RequestMapping(value = "/getetlrule", method = RequestMethod.GET, produces = "application/json;charset=utf-8")
    @ResponseBody
    public String getETL() {
        String redisKey = "etl:weatherElement";
        Map<String,String> result = jedisCluster.hgetAll(redisKey);
        String  o = JSONObject.toJSONString(result);
        return o;
    }
}
