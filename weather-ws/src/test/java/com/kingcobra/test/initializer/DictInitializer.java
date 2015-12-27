package com.kingcobra.test.initializer;

import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.weatherws.common.Constant;
import redis.clients.jedis.JedisCluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kingcobra on 15/12/21.
 */
public class DictInitializer {
    private static final RedisConnector redisConnector = RedisConnector.Builder.build();
    private static final JedisCluster jedisCluster = redisConnector.getJedisCluster();

    public void weatherInitialize() {
        String dictName = Constant.DICT_WEATHER;
        try {
            File file = new File("/Users/kingcobra/Downloads/weathercode");
            Reader reader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String s;
            String[] v;
            JSONObject code = new JSONObject();
            Map<String, String> inputData = new HashMap<String, String>();
            while ((s = bufferedReader.readLine()) != null) {
                s = s.replaceAll("\\s+", ",");
                v = s.split(",");
                code.put("name", v[0]);
                inputData.put(v[1], code.toJSONString());
                System.out.println(code);

            }
            jedisCluster.hmset(dictName, inputData);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void getWeatherDict() {
        String dictName = Constant.DICT_WEATHER;
        Map<String,String> value  =jedisCluster.hgetAll(dictName);
        for (Map.Entry<String, String> entry : value.entrySet()) {
            System.out.println(entry.getKey()+","+entry.getValue());
        }
    }

    public void ffInitializer() {
        String dictName = Constant.DICT_FF;
        try {
            Map<String, String> inputData = new HashMap<String, String>();
            inputData.put("00", "{\"name\":\"微风\",\"ename\":\"<10m/h\"}");
            inputData.put("01", "{\"name\":\"3-4级\",\"ename\":\"10~17m/h\"}");
            inputData.put("02", "{\"name\":\"4-5级\",\"ename\":\"17~25m/h\"}");
            inputData.put("03", "{\"name\":\"5-6级\",\"ename\":\"25~34m/h\"}");
            inputData.put("04", "{\"name\":\"6-7级\",\"ename\":\"34~43m/h\"}");
            inputData.put("05", "{\"name\":\"7-8级\",\"ename\":\"43~54m/h\"}");
            inputData.put("06", "{\"name\":\"8-9级\",\"ename\":\"54~65m/h\"}");
            inputData.put("07", "{\"name\":\"9-10级\",\"ename\":\"65~77m/h\"}");
            inputData.put("08", "{\"name\":\"10-11级\",\"ename\":\"77~89m/h\"}");
            inputData.put("09", "{\"name\":\"11-12级\",\"ename\":\"89~102m/h\"}");
            jedisCluster.hmset(dictName, inputData);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getFFDict() {
        String dictName = Constant.DICT_FF;
        Map<String,String> value  =jedisCluster.hgetAll(dictName);
        for (Map.Entry<String, String> entry : value.entrySet()) {
            System.out.println(entry.getKey()+","+entry.getValue());
            JSONObject v = JSONObject.parseObject(entry.getValue());
            System.out.println(v.getString("name"));
        }
    }

    public void ddInitializer() {
        String dictName = Constant.DICT_DD;
        try {
            Map<String, String> inputData = new HashMap<String, String>();
            inputData.put("00", "{\"name\":\"无持续风向\"}");
            inputData.put("01", "{\"name\":\"东北风\"}");
            inputData.put("02", "{\"name\":\"东风\"}");
            inputData.put("03", "{\"name\":\"东南风\"}");
            inputData.put("04", "{\"name\":\"南风\"}");
            inputData.put("05", "{\"name\":\"西南风\"}");
            inputData.put("06", "{\"name\":\"西风\"}");
            inputData.put("07", "{\"name\":\"西北风\"}");
            inputData.put("08", "{\"name\":\"北风\"}");
            inputData.put("09", "{\"name\":\"旋转风\"}");
            jedisCluster.hmset(dictName, inputData);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getDDDict() {
        String dictName = Constant.DICT_DD;
        Map<String,String> value  =jedisCluster.hgetAll(dictName);
        for (Map.Entry<String, String> entry : value.entrySet()) {
            System.out.println(entry.getKey()+","+entry.getValue());
            JSONObject v = JSONObject.parseObject(entry.getValue());
            System.out.println(v.getString("name"));
        }
    }
    public static void main(String[] args) {
        DictInitializer dictInitializer = new DictInitializer();
      /*  dictInitializer.weatherInitialize();
        dictInitializer.getWeatherDict();*/
        dictInitializer.ffInitializer();
        dictInitializer.getFFDict();
      /*  dictInitializer.ddInitializer();
        dictInitializer.getDDDict();*/

    }
}
