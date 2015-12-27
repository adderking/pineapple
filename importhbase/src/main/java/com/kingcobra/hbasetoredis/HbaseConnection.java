package com.kingcobra.hbasetoredis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by kingcobra on 15/12/26.
 */
public class HbaseConnection {
    private static final Configuration configuration = HBaseConfiguration.create();
    private static HConnection hConnection;
    private static Lock lock = new ReentrantLock();
    private static final Logger log = LoggerFactory.getLogger(HbaseConnection.class);
    static{
       /* configuration.set("hbase.zookeeper.quorum", "hadoop4:2181,hadoop5:2181,hadoop6:2181");
        configuration.set("hbase.rootdir","hdfs://hadoop4:9000/hbase");*/
        configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        configuration.set("hbase.rootdir","hdfs://hadoop1:9000/hbase");
    }
    public static HConnection getConnection(){
        synchronized (lock) {
            try {
                if (hConnection == null) {
                    hConnection = HConnectionManager.createConnection(configuration);
                }
                return hConnection;
            } catch ( ZooKeeperConnectionException e) {
                log.error("hbase connection is errro :{}" + e);
                return null;
            } catch (IOException e) {
                log.error("hbase connection is errro :{}" + e);
                return null;
            }
        }
    }

    public static Configuration getConfiguration() {
        return configuration;
    }
}
