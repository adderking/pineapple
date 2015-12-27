package com.kingcobra.hbasetoredis;


import com.kingcobra.kedis.core.RedisConnector;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.*;

/**
 * Created by kingcobra on 15/12/26.
 */
public class ImportData {
    private HConnection connection = HbaseConnection.getConnection();
    private static final Logger log = LoggerFactory.getLogger(ImportData.class);
    private static RedisConnector redisConnector = RedisConnector.Builder.build();
    private static final String areaTable = "area.internal:%s";
    public void  internalStationToRedis(int num) throws IOException {

        String key = null;
        HTableInterface table = connection.getTable("dict_station");
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("station"));
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        JedisCluster jedisCluster = redisConnector.getJedisCluster();
        int i = 0;
        while ((result = scanner.next()) != null) {
            Map<String, String> data = new HashMap<String, String>();
            String rowkey = Bytes.toString(result.getRow());
            key = String.format(areaTable, rowkey);
            NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("station"));
            for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                data.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
            log.info("result {} is added ",i++);
            jedisCluster.hmset(key, data);
        }
    }

    public void initStationTable() throws IOException {
        byte[] cf = Bytes.toBytes("station");
        HTable table = new HTable(HbaseConnection.getConfiguration(), "dict_station");
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 2; i++) {
            Put put = new Put(Bytes.toBytes("101010100" + i));
            put.add(cf, Bytes.toBytes("stationid"), Bytes.toBytes("54511" + i));
            put.add(cf, Bytes.toBytes("namecn"), Bytes.toBytes("北京" + i));
            put.add(cf, Bytes.toBytes("nameen"), Bytes.toBytes("beijing"));
            put.add(cf, Bytes.toBytes("height"), Bytes.toBytes("32.50"));
            puts.add(put);
        }
        table.put(puts);
        table.flushCommits();
    }

    public void createTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(connection);
        HTableDescriptor desc = new HTableDescriptor("dict_station");
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("station");
        desc.addFamily(columnDescriptor);
        desc.addFamily(columnDescriptor);
        admin.createTable(desc);
    }
    public static void main(String[] args) {
        ImportData importData = new ImportData();
        try {
//            importData.initStationTable();
            int num=0;
            if(args.length!=0){
                num=Integer.valueOf(args[0]);
            }
            importData.internalStationToRedis(num);
        } catch (IOException e) {
            log.error("error message :{}",e);
        }

    }
}
