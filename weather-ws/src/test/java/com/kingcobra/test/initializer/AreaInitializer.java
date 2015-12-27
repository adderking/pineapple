package com.kingcobra.test.initializer;

import com.kingcobra.kedis.core.RedisConnector;
import com.kingcobra.weatherws.common.Constant;
import redis.clients.jedis.JedisCluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/12/21.
 */
public class AreaInitializer {
    private static final String FILENAME = "/Users/kingcobra/Downloads/area.csv";
    private static final RedisConnector redisConnector = RedisConnector.Builder.build();
    private static final JedisCluster jedisCluster = redisConnector.getJedisCluster();

    private static final String AREATABLENAME = Constant.AREA_TABLE;
    private static final String stationsTable = "internal.stations";
    Pattern pattern = Pattern.compile("\\d+");
    public void areaInitialize() {
        try {
            FileReader reader = new FileReader(new File(FILENAME));
            BufferedReader bufferedReader = new BufferedReader(reader);
            String s ,areaId;
            Map<String, String> data = new HashMap<String, String>();
            String[] value ;
            int num = 0;
            Set<String> stationSet = new HashSet<String>();

            while ((s = bufferedReader.readLine()) != null) {
                value = s.split(",");
                areaId = value[1];
                Matcher matcher = pattern.matcher(areaId);
                if(!matcher.matches()){
                    continue;
                }
                String key = String.format(AREATABLENAME, areaId);
                data.put("stationId", value[0]);
                data.put("areaId", areaId);
                data.put("nameen", value[2]);
                data.put("namecn", value[3]);
                String status = jedisCluster.hmset(key, data);
                stationSet.add(value[0]);
            }
            String temp[] = new String[stationSet.size()];
            jedisCluster.sadd(stationsTable, stationSet.toArray(temp));
            bufferedReader.close();
            reader.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void aroundAreaInitialize() {

    }
    public void getAreaByID() {
        String areaID = "101220503";
        String key = String.format(AREATABLENAME, areaID);
        Map<String,String> data = jedisCluster.hgetAll(key);
        System.out.println(data.toString());
    }

    public void getInternalStations() {
        Set<String> stationIds = jedisCluster.smembers(stationsTable);
        System.out.println(stationIds.size());
    }

    public static void main(String[] args) {
        AreaInitializer areaInitializer = new AreaInitializer();
        areaInitializer.areaInitialize();
        areaInitializer.getAreaByID();
        areaInitializer.getInternalStations();
    }


}
