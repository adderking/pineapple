import com.kingcobra.kedis.core.RedisConnector;
import redis.clients.jedis.JedisCluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by kingcobra on 15/9/28.
 */
public class TestReadDataFile {
    public static void read3hExternalFile() {
        File file = new File("/Users/kingcobra/Documents/works/forecast_fine_3h_external_201507311111.txt");
        Set<String> stations = new HashSet<String>();
        try {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String content;
            String stationId;
            while ((content =bufferedReader.readLine()) != null) {
                System.out.println(content);
                stationId = content.split(",")[0];
                stations.add(stationId);
            }
            bufferedReader.close();
            fileReader.close();
            System.out.println("------"+stations.size());
            for (String s : stations) {
                System.out.println(s);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void initDictStations() {
        File file = new File("/Users/kingcobra/Documents/works/forecast_fine_3h_external_201507311111.txt");
//        File file = new File("/Users/kingcobra/Documents/works/DICT_STATION_2676.csv");
        String key = "external.stations";
        RedisConnector redisConnector = RedisConnector.Builder.build();
        JedisCluster jedisCluster = redisConnector.getJedisCluster();
//        jedisCluster.del(key);
        Set<String> stations = new HashSet<String>();
        try {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String content;
            String stationId;
            while ((content =bufferedReader.readLine()) != null) {
                stationId = content.split(",")[0];
               /* if("3005".equals(stationId)||"3166".equals(stationId)||"3772".equals(stationId))
                    System.out.println(stationId);
                stations.add(stationId);*/
                jedisCluster.sadd(key, stationId);
            }
            bufferedReader.close();
            fileReader.close();
            redisConnector.closeJedisCluster();
          /*  System.out.println(stations.size());
            for (String s : stations) {
                System.out.println(s);

            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        TestReadDataFile.read3hExternalFile();
//        TestReadDataFile.initDictStations();
    }

}
