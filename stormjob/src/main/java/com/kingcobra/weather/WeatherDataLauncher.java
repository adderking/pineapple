package com.kingcobra.weather;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.kingcobra.kedis.core.RedisConnector;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import redis.clients.jedis.JedisCluster;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by kingcobra on 15/10/11.
 */
public class WeatherDataLauncher {
    private static final RedisConnector REDISCONNECTOR= RedisConnector.Builder.build();
    private TopologyBuilder builder = new TopologyBuilder();
    private final JedisCluster jedisCluster=REDISCONNECTOR.getJedisCluster();
    public void launch(String[] args) throws Exception{
        Config config = new Config();
        config.setMessageTimeoutSecs(60 * 5);
        Map<String, String> hbaseConfig = new HashMap<String, String>();
        hbaseConfig.put(Constant.HBASE_ROOT_KEY, Constant.HBASE_ROOT_VALUE);
        hbaseConfig.put(Constant.HBASE_ZOOKEEPER_KEY, Constant.HBASE_ZOOKEEPER_VALUE);
        config.put(Constant.HBASE_CONFIG,hbaseConfig);
        String topologyName = args[0];
        String kafkaTopic = args[1];
        String persistPolicyName = args[2];
        buildTopology(topologyName, kafkaTopic, persistPolicyName);
        /*LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(topologyName,config,builder.createTopology());*/
        StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
    }

    private void buildTopology(String topologyName,String kafkaTopic, String dataStructureName) throws Exception {
        addSpout(topologyName, kafkaTopic);
        addBolt(kafkaTopic, dataStructureName);
    }

    /**
     * add spout to TopologyBuilder
     * @param topologyName
     * @param kafkaTopic
     */
    private void addSpout(String topologyName,String kafkaTopic) {
        BrokerHosts brokerHosts = new ZkHosts(Constant.KAFKA_ZKSTR);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,kafkaTopic, "/" + topologyName, UUID.randomUUID().toString());
        spoutConfig.fetchMaxWait=Constant.SPOUTCONFIG_FETCHMAXWAIT;
        spoutConfig.socketTimeoutMs=Constant.SPOUTCONFIG_SOCKETTIMEOUTMS;
        spoutConfig.forceFromStart=Constant.SPOUTCONFIG_FORCEFROMSTART;
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafkaSpout", kafkaSpout, 3);
    }

    /**
     * add bolt to TopologyBuilder
     * @param kafkaTopic
     * @param persistPolicyName
     */
    private void addBolt(String kafkaTopic, String persistPolicyName) throws Exception {
        persistPolicyName = String.format(Constant.PERSIST_TABLE_NAME_FORMAT, persistPolicyName);
        Map<String,String> persistPolicy = jedisCluster.hgetAll(persistPolicyName);
        if (persistPolicy == null||persistPolicy.size()==0) {
            throw new Exception("dataStruncture is not found");
        }
        String persistColumns = persistPolicy.get("columns");
        String s_persistTarget = persistPolicy.get("type");
        if (Strings.isNullOrEmpty(persistColumns) || Strings.isNullOrEmpty(s_persistTarget)) {
            throw new Exception("dataStruncture is error");
        }
        JSONArray columns = JSONArray.parseArray(persistColumns);
        EtlBolt etlBolt = new EtlBolt(persistPolicyName,columns);
        builder.setBolt("etlBolt", etlBolt,Constant.ETLBOLT_EXECUTOR_NUM).shuffleGrouping("kafkaSpout");

        JSONArray ja_persistTarget = JSONArray.parseArray(s_persistTarget);
        JSONObject targetConfig=null,param=null;
        for (int i = 0; i < ja_persistTarget.size(); i++) {
            targetConfig = ja_persistTarget.getJSONObject(i);
            String type = targetConfig.getString("name");
            param = targetConfig.getJSONObject("params");
            if (type.equalsIgnoreCase(PersistType.redis.name())) {
                String key = param.getString("key");
                String recordIdentifier = param.getString("recordIdentifier");
                String tableName = jedisCluster.hget("persistTarget:" + kafkaTopic, PersistType.redis.name());
                WriteRedisBolt writeRedisBolt = new WriteRedisBolt(tableName,key,recordIdentifier);
                builder.setBolt("writeRedisBolt", writeRedisBolt,Constant.WRITEREDISBOLT_EXECUTOR_NUM).shuffleGrouping("etlBolt");
            }
            if (type.equalsIgnoreCase(PersistType.hbase.name())) {
                String key = param.getString("key");
                String hbaseTable = jedisCluster.hget("persistTarget:" + kafkaTopic, PersistType.hbase.name());
                JSONObject hbaseTabelInfo = JSONObject.parseObject(hbaseTable);
                String tableName = hbaseTabelInfo.getString("tableName");
                String columnF = hbaseTabelInfo.getString("columnFamily");
                HBaseMapper hBaseMapper = new WeatherHbaseMapper(key,columnF);
                HBaseBolt hBaseBolt = new HBaseBolt(tableName,hBaseMapper);
                hBaseBolt.writeToWAL(Constant.WRITE_WAL);
                hBaseBolt.withConfigKey(Constant.HBASE_CONFIG);
                builder.setBolt("writeHbaseBolt", hBaseBolt,Constant.WRITEHBASEBOLT_EXECUTOR_NUM).shuffleGrouping("etlBolt");
            }
        }
    }


    private static class Constant{
        /*static final String KAFKA_ZKSTR = "hadoop4:2181,hadoop5:2181,hadoop6:2181";

        static final int SPOUTCONFIG_FETCHMAXWAIT = 1000;
        static final int SPOUTCONFIG_SOCKETTIMEOUTMS = 60000;
        static final boolean SPOUTCONFIG_FORCEFROMSTART = false;//设置是否重头读取kafka中数据
        static final int ETLBOLT_EXECUTOR_NUM=4;
        static final int WRITEREDISBOLT_EXECUTOR_NUM=3;
        static final int WRITEHBASEBOLT_EXECUTOR_NUM=3;

        static final String PERSIST_TABLE_NAME_FORMAT = "persistPolicy:%s";

        static final String HBASE_CONFIG = "hbaseConfig";
        static final String HBASE_ROOT_KEY = "hbase.rootdir";
        static final String HBASE_ROOT_VALUE = "hdfs://hadoop4:9000/hbase";
        static final String HBASE_ZOOKEEPER_KEY = "hbase.zookeeper.quorum";
        static final String HBASE_ZOOKEEPER_VALUE = "hadoop4,hadoop5,hadoop6";
        static final boolean WRITE_WAL = false; //写hbase时是否写WAL*/

        static final String KAFKA_ZKSTR = "IP1:2181,IP2:2181,IP3:2181";

        static final int SPOUTCONFIG_FETCHMAXWAIT = 1000;
        static final int SPOUTCONFIG_SOCKETTIMEOUTMS = 60000;
        static final boolean SPOUTCONFIG_FORCEFROMSTART = false;//设置是否重头读取kafka中数据
        static final int ETLBOLT_EXECUTOR_NUM=4;
        static final int WRITEREDISBOLT_EXECUTOR_NUM=3;
        static final int WRITEHBASEBOLT_EXECUTOR_NUM=3;

        static final String PERSIST_TABLE_NAME_FORMAT = "persistPolicy:%s";

        static final String HBASE_CONFIG = "hbaseConfig";
        static final String HBASE_ROOT_KEY = "hbase.rootdir";
        static final String HBASE_ROOT_VALUE = "hdfs://IP1:9000/hbase";
        static final String HBASE_ZOOKEEPER_KEY = "hbase.zookeeper.quorum";
        static final String HBASE_ZOOKEEPER_VALUE = "IP1,IP2,IP3";
        static final boolean WRITE_WAL = false; //写hbase时是否写WAL

    }
    /**
     * persist Type :redis hbase
     */
    private enum PersistType{
        redis,hbase
    }
    /**
     * args[0] topoloy name
     * args[1] kafka Topic name
     * args[2] persistPolicy name
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("args is error");
            System.exit(-1);
        }
        WeatherDataLauncher weatherData = new WeatherDataLauncher();
        try {
            weatherData.launch(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
