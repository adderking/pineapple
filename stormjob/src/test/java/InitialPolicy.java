import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.kingcobra.kedis.core.RedisConnector;
import org.apache.avro.generic.GenericData;
import redis.clients.jedis.JedisCluster;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by kingcobra on 15/10/14.
 */
public class InitialPolicy {
    private static final RedisConnector REDIS_CONNECTOR = RedisConnector.Builder.build();
    private JedisCluster jedisCluster;

    /**
     * 初始化pmsc格式的持久化规则
     */
    public void persistPolicy_Pmsc() {
        jedisCluster = REDIS_CONNECTOR.getJedisCluster();
        String redisKey = "persistPolicy:pmsc_fine_12h";
        String columns = "[\"stationID\",\"lon\",\"lat\",\"height\",\"$t\",\"$t\",\"report_time(LST)\",\"$t\",\"$t\",\"$t\",\"TIME_STEP\",\"UTC\",\"LST\",\"TEMP\",\"TEMP_MAX\",\"TEMP_MIN\",\"RAIN\",\"FF\",\"FF_LEVEL\",\"DD\",\"DD_LEVEL\",\"CLOUD\",\"WEATHER\",\"RH\"]";
        JSONObject persistTarget = new JSONObject();
        persistTarget.put("name", "redis");
        JSONObject targetParams = new JSONObject();
        targetParams.put("key", "stationID");
        targetParams.put("recordIdentifier", "TIME_STEP");
        JSONArray targets = new JSONArray();
        persistTarget.put("params", targetParams);
        targets.add(persistTarget);

        persistTarget = new JSONObject();
        persistTarget.put("name", "hbase");
        targetParams = new JSONObject();
        targetParams.put("key", "stationID.LST.TIME_STEP");
        targetParams.put("columnF", "weatherdata");
        persistTarget.put("params", targetParams);

        targets.add(persistTarget);


        jedisCluster.hset(redisKey, "columns", columns);
        jedisCluster.hset(redisKey, "target", targets.toJSONString());

    }

    /**
     * 删除PMSC格式持久化规则中的persistTarget字段
     */
    public void delPersistPolicy_Pmsc() {
        jedisCluster = REDIS_CONNECTOR.getJedisCluster();
        String redisKey = "persistPolicy:pmsc";
        jedisCluster.del(redisKey);
    }

    public void initPersistTarget_pmsc_3h() {
        jedisCluster = REDIS_CONNECTOR.getJedisCluster();
        String kafkaTopic = "pmsc_fine";
        String redisTableName = "pmsc_fine_3h";
        String hbaseTableName = redisTableName;
        String hbaseTableColumnF = "weatherdata";
        JSONObject hbaseInfo = new JSONObject();
        hbaseInfo.put("tableName", hbaseTableName);
        hbaseInfo.put("columnFamily", hbaseTableColumnF);
        jedisCluster.hset("persistTarget:pmsc_fine", "redis", redisTableName);
        jedisCluster.hset("persistTarget:pmsc_fine", "hbase", hbaseInfo.toJSONString());

    }

    /**
     * 初始化清洗规则
     */
    public void persistWeatherElementEtlPolicy() {
        jedisCluster = REDIS_CONNECTOR.getJedisCluster();
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
    public void getPmscPersist() {
        jedisCluster = REDIS_CONNECTOR.getJedisCluster();
        String redisKey = "persistPolicy:pmsc";
        String columns=jedisCluster.hget(redisKey, "columns");
        JSONArray ja_columns = JSONArray.parseArray(columns);
        for (int i = 0; i < ja_columns.size(); i++) {
            System.out.println(ja_columns.get(i));
        }
        String persistTarget = jedisCluster.hget(redisKey, "target");
        System.out.println(persistTarget);
    }

    public void getWeatherElementEtlPolicy() {
        jedisCluster = REDIS_CONNECTOR.getJedisCluster();
        String redisKey = "etl:weatherElement";
        Map<String, String> etl = jedisCluster.hgetAll(redisKey);
        for (Map.Entry<String, String> entry : etl.entrySet()) {
            System.out.println(entry.getKey());
            JSONObject o = JSONObject.parseObject(entry.getValue());
            System.out.println(o);
            Double v = o.getDoubleValue("min");
            System.out.println(v);
        }
        Double _v = Double.valueOf(50.0);
        System.out.println(_v);
        System.out.println(_v.toString());

    }
    public void close() {
        REDIS_CONNECTOR.closeJedisCluster();
    }

    public static void main(String[] args) {
        InitialPolicy initialPolicy = new InitialPolicy();
        /*initialPolicy.persistPolicy_Pmsc();
        initialPolicy.getPmscPersist();
        initialPolicy.initPersistTarget_pmsc_3h();*/
        initialPolicy.persistWeatherElementEtlPolicy();
        initialPolicy.getWeatherElementEtlPolicy();
//        initialPolicy.delPersistPolicy_Pmsc();
        initialPolicy.close();
    }
    private enum PersistType {
        Redis,Hbase
    }
}
