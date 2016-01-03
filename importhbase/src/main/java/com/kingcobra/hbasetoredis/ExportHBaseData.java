package com.kingcobra.hbasetoredis;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.kedis.core.RedisConnector;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.*;
import java.util.*;

/**
 * Created by kingcobra on 15/12/26.
 */
public class ExportHBaseData {
    private HConnection connection = HbaseConnection.getConnection();
    private static final Logger log = LoggerFactory.getLogger(ExportHBaseData.class);
    private RedisConnector redisConnector;
    private static final String areaTable = "area.internal:%s";

    public ExportHBaseData() {
        Properties properties=new Properties();
        try {
            InputStream inputStream = ExportHBaseData.class.getClassLoader().getResourceAsStream("system.properties");
            properties.load(inputStream);
            redisConnector = RedisConnector.Builder.build(properties);
        }catch(IOException ioException){
            log.error(ioException.getMessage());
        }
    }
    public void  internalStationToRedis() throws Exception {

        String key = null;
        HTableInterface table = connection.getTable("dict_station");
        Scan scan = new Scan();
        scan.setAttribute("LIMIT",Bytes.toBytes(1));
        scan.addFamily(Bytes.toBytes("station"));
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        JedisCluster jedisCluster = redisConnector.getJedisCluster();
        int i = 0;
        FileWriter writer = new FileWriter(new File("/home/eray/pineapple/internalarea.json"));
        while ((result = scanner.next()) != null) {
            Map<String, String> data = new HashMap<String, String>();
            String rowkey = Bytes.toString(result.getRow());
            NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("station"));
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                data.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
            String s = JSONObject.toJSONString(data);
            JSONObject r = new JSONObject();
            r.put("rowkey", rowkey);
            r.put("data", s);
            writer.write(r.toJSONString() + "\n");
        }
        writer.close();
        table.close();
    }

    public void internalAroundStation() throws Exception {
        String key = null;
        HTableInterface table = connection.getTable("dict_station");
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("stationAround"));
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        FileWriter writer = new FileWriter(new File("/home/eray/pineapple/internalAroundArea.json"));
        while ((result = scanner.next()) != null) {
            Map<String, String> data = new HashMap<String, String>();
            String rowkey = Bytes.toString(result.getRow());
            NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("stationAround"));
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                data.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
            String s = JSONArray.toJSONString(data);
            JSONObject r = new JSONObject();
            r.put("rowkey", rowkey);
            r.put("data", s);
            writer.write(r.toJSONString() + "\n");
        }
        writer.close();
        table.close();

    }
    public void  externalStationToRedis() throws Exception {

        String key = null;
        HTableInterface table = connection.getTable("dict_station_foreign");
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("station"));
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        JedisCluster jedisCluster = redisConnector.getJedisCluster();
        int i = 0;
        FileWriter writer = new FileWriter(new File("/home/eray/pineapple/externalarea.json"));
        while ((result = scanner.next()) != null) {
            Map<String, String> data = new HashMap<String, String>();
            String rowkey = Bytes.toString(result.getRow());
            NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("station"));
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                data.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
            String s = JSONObject.toJSONString(data);
            JSONObject r = new JSONObject();
            r.put("rowkey", rowkey);
            r.put("data", s);
            writer.write(r.toJSONString() + "\n");
        }
        writer.close();
        table.close();
    }

    public void externalAroundStation() throws Exception {
        String key = null;
        HTableInterface table = connection.getTable("dict_station_foreign");
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("stationAround"));
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        FileWriter writer = new FileWriter(new File("/home/eray/pineapple/externalAroundArea.json"));
        while ((result = scanner.next()) != null) {
            Map<String, String> data = new HashMap<String, String>();
            String rowkey = Bytes.toString(result.getRow());
            NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("stationAround"));
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                data.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
            String s = JSONArray.toJSONString(data);
            JSONObject r = new JSONObject();
            r.put("rowkey", rowkey);
            r.put("data", s);
            writer.write(r.toJSONString() + "\n");
        }
        writer.close();
        table.close();
    }
    public void  travelStationToRedis() throws Exception {

        String key = null;
        HTableInterface table = connection.getTable("dict_internal_travel");
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("travel"));
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        JedisCluster jedisCluster = redisConnector.getJedisCluster();
        int i = 0;
        FileWriter writer = new FileWriter(new File("/home/eray/pineapple/travelarea.json"));
        while ((result = scanner.next()) != null) {
            Map<String, String> data = new HashMap<String, String>();
            String rowkey = Bytes.toString(result.getRow());
            NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("travel"));
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                data.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
            String s = JSONObject.toJSONString(data);
            JSONObject r = new JSONObject();
            r.put("rowkey", rowkey);
            r.put("data", s);
            writer.write(r.toJSONString() + "\n");
        }
        writer.close();
        table.close();
    }

    public void travelAroundStation() throws Exception {
        String key = null;
        HTableInterface table = connection.getTable("dict_internal_travel");
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("stationAround"));
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        FileWriter writer = new FileWriter(new File("/home/eray/pineapple/travelAroundArea.json"));
        while ((result = scanner.next()) != null) {
            Map<String, String> data = new HashMap<String, String>();
            String rowkey = Bytes.toString(result.getRow());
            NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("stationAround"));
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                data.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
            String s = JSONArray.toJSONString(data);
            JSONObject r = new JSONObject();
            r.put("rowkey", rowkey);
            r.put("data", s);
            writer.write(r.toJSONString() + "\n");
        }
        writer.close();
        table.close();
    }

    public static void main(String[] args) {
        ExportHBaseData importData = new ExportHBaseData();
        try {
//            importData.initStationTable();
            importData.internalStationToRedis();
            importData.internalAroundStation();
            importData.externalStationToRedis();
            importData.externalAroundStation();
            importData.travelStationToRedis();
            importData.travelAroundStation();
        } catch (Exception e) {
            log.error("start error:{}",e);
        }

    }
}
