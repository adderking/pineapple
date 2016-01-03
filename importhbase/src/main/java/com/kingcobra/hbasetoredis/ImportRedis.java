package com.kingcobra.hbasetoredis;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.kingcobra.kedis.core.RedisConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.*;
import java.util.*;

/**
 * Created by kingcobra on 15/12/28.
 */
public class ImportRedis {
    private RedisConnector redisConnector;
    private JedisCluster jedisCluster;
    //    String path = "/Users/kingcobra/Downloads/json/";
    String path = "";
    private static final Logger log = LoggerFactory.getLogger(ImportRedis.class);
    public ImportRedis() {
        Properties properties=new Properties();
        try {
            InputStream inputStream = ExportHBaseData.class.getClassLoader().getResourceAsStream("system.properties");
            properties.load(inputStream);
            redisConnector = RedisConnector.Builder.build(properties);
        }catch(IOException ioException){
            log.error(ioException.getMessage());
        }
        jedisCluster = redisConnector.getJedisCluster();
    }

    public void importInternalArea() throws IOException {
        String keyTemplate = "area.internal:%s";
        FileReader reader = new FileReader(new File(path+"internalarea.json"));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            JSONObject rowData = JSONObject.parseObject(line);
            String rowkey = rowData.getString("rowkey");
            String redisKey = String.format(keyTemplate, rowkey);
            String data = rowData.getString("data");
            JSONObject d = JSONObject.parseObject(data);
            Set<String> k = d.keySet();
            String value;
            Map<String, String> toRedis = new HashMap<String, String>();
            for (String s : k) {
                value = d.getString(s);
                if (Strings.isNullOrEmpty(value)) {
                    continue;
                }
                if ("stationid".equals(s)) {
                    s = "stationId";
                }
                if ("areaid".equals(s)) {
                    s = "areaId";
                }
                toRedis.put(s, value);
            }
            jedisCluster.hmset(redisKey, toRedis);
        }
    }

    public void importInternalAroundArea() throws Exception {
        String keyTemplate = "area.internal.around:%s";
        FileReader reader = new FileReader(new File(path+"internalAroundArea.json"));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            JSONObject rowData = JSONObject.parseObject(line);
            String rowkey = rowData.getString("rowkey");
            String redisKey = String.format(keyTemplate, rowkey);
            String data = rowData.getString("data");
            JSONObject d = JSONObject.parseObject(data);
            Set<String> k = d.keySet();
            String areaid,stationid;
            Set<String> toRedis = new HashSet<String>();
            String k_prefix = "around_areaid_";
            for (String s :k) {
                if (s.startsWith(k_prefix)) {
                    String suffix = s.replaceAll(k_prefix, "");
                    String kk = "around_stationid_" + suffix;
                    areaid = d.getString(s);
                    stationid = d.getString(kk);
                    JSONObject o = new JSONObject();
                    o.put("areaId",areaid);
                    o.put("stationId", stationid);
                    toRedis.add(o.toJSONString());
                }

            }
            String array[] = new String[toRedis.size()];
            toRedis.toArray(array);
//            jedisCluster.srem(redisKey, array);
            Map<String, Double> dataAndScore = new HashMap<String, Double>();
            for (int i=0;i<array.length;i++) {
                String _k = array[i];
                Double _s = Double.valueOf(i + "");
                dataAndScore.put(_k, _s);
            }
            jedisCluster.zadd(redisKey, dataAndScore);
        }
    }
    public void importExternalArea() throws IOException {
        String keyTemplate = "area.external:%s";
        FileReader reader = new FileReader(new File(path+"externalarea.json"));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            JSONObject rowData = JSONObject.parseObject(line);
            String rowkey = rowData.getString("rowkey");
            String redisKey = String.format(keyTemplate, rowkey);
            String data = rowData.getString("data");
            JSONObject d = JSONObject.parseObject(data);
            Set<String> k = d.keySet();
            String value;
            Map<String, String> toRedis = new HashMap<String, String>();
            for (String s : k) {
                value = d.getString(s);
                if (Strings.isNullOrEmpty(value)) {
                    continue;
                }
                if ("stationid".equals(s)) {
                    s = "stationId";
                }
                if ("areaid".equals(s)) {
                    s = "areaId";
                }
                toRedis.put(s, value);
            }
            jedisCluster.hmset(redisKey, toRedis);
        }
    }
    public void importExternalAroundArea() throws Exception {
        String keyTemplate = "area.external.around:%s";
        FileReader reader = new FileReader(new File(path+"externalAroundArea.json"));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            JSONObject rowData = JSONObject.parseObject(line);
            String rowkey = rowData.getString("rowkey");
            String redisKey = String.format(keyTemplate, rowkey);
            String data = rowData.getString("data");
            JSONObject d = JSONObject.parseObject(data);
            Set<String> k = d.keySet();
            String areaid,stationid;
            Set<String> toRedis = new HashSet<String>();
            String k_prefix = "around_areaid_";
            for (String s :k) {
                if (s.startsWith(k_prefix)) {
                    String suffix = s.replaceAll(k_prefix, "");
                    String kk = "around_stationid_" + suffix;
                    areaid = d.getString(s);
                    stationid = d.getString(kk);
                    JSONObject o = new JSONObject();
                    o.put("areaId",areaid);
                    o.put("stationId", stationid);
                    toRedis.add(o.toJSONString());
                }

            }
            String array[] = new String[toRedis.size()];
            toRedis.toArray(array);
//            jedisCluster.srem(redisKey, array);
            Map<String, Double> dataAndScore = new HashMap<String, Double>();
            for (int i=0;i<array.length;i++) {
                String _k = array[i];
                Double _s = Double.valueOf(i + "");
                dataAndScore.put(_k, _s);
            }
            jedisCluster.zadd(redisKey, dataAndScore);
        }
    }
    public void importTravelArea() throws IOException {
        String keyTemplate = "travel.internal:%s";
        FileReader reader = new FileReader(new File(path+"travelarea.json"));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            JSONObject rowData = JSONObject.parseObject(line);
            String rowkey = rowData.getString("rowkey");
            String redisKey = String.format(keyTemplate, rowkey);
            String data = rowData.getString("data");
            JSONObject d = JSONObject.parseObject(data);
            Set<String> k = d.keySet();
            String value;
            Map<String, String> toRedis = new HashMap<String, String>();
            for (String s : k) {
                value = d.getString(s);
                if (Strings.isNullOrEmpty(value)) {
                    continue;
                }
                if ("stationid".equals(s)) {
                    s = "stationId";
                }
                if ("areaid".equals(s)) {
                    s = "areaId";
                }
                if ("tareaid".equals(s)) {
                    s = "tareaId";
                }
                toRedis.put(s, value);
            }
            jedisCluster.hmset(redisKey, toRedis);
        }
    }
    public void importTravelAroundArea() throws Exception {
        String keyTemplate = "travel.internal.around:%s";
        FileReader reader = new FileReader(new File(path+"travelAroundArea.json"));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            JSONObject rowData = JSONObject.parseObject(line);
            String rowkey = rowData.getString("rowkey");
            String redisKey = String.format(keyTemplate, rowkey);
            String data = rowData.getString("data");
            JSONObject d = JSONObject.parseObject(data);
            Set<String> k = d.keySet();
            String areaid,stationid;
            Set<String> toRedis = new HashSet<String>();
            String k_prefix = "around_tareaid_";
            for (String s :k) {
                if (s.startsWith(k_prefix)) {
                    String suffix = s.replaceAll(k_prefix, "");
                    String kk = "around_stationid_" + suffix;
                    areaid = d.getString(s);
                    stationid = d.getString(kk);
                    JSONObject o = new JSONObject();
                    o.put("areaId",areaid);
                    o.put("stationId", stationid);
                    toRedis.add(o.toJSONString());
                }

            }
            String array[] = new String[toRedis.size()];
            toRedis.toArray(array);
//            jedisCluster.srem(redisKey, array);
            Map<String, Double> dataAndScore = new HashMap<String, Double>();
            for (int i=0;i<array.length;i++) {
                String _k = array[i];
                Double _s = Double.valueOf(i + "");
                dataAndScore.put(_k, _s);
            }
            jedisCluster.zadd(redisKey, dataAndScore);
        }
    }
    public void weatherInitialize() {
        String dictName = "dict:weather";
        try {
            File file = new File("weathercode");
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
    public void ffInitializer() {
        String dictName = "dict:ff";
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
    public void ddInitializer() {
        String dictName = "dict:dd";
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
    public static void main(String[] args) {
        ImportRedis importRedis = new ImportRedis();
        try {
            importRedis.importInternalArea();
            importRedis.importExternalArea();
            importRedis.importTravelArea();
            importRedis.importInternalAroundArea();
            importRedis.importExternalAroundArea();
            importRedis.importTravelAroundArea();
            importRedis.weatherInitialize();
            importRedis.ddInitializer();
            importRedis.ffInitializer();
        } catch (Exception e) {
            log.error("error msg:", e);
        }

    }
}
