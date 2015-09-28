package com.kingcobra.flume.monitor;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by kingcobra on 15/9/20.
 */
public class StationMonitor extends AbstractEventMonitor{

    private static final Logger LOGGER = LoggerFactory.getLogger(StationMonitor.class);

    public StationMonitor(Context ctx) {
       super(ctx);
    }

    public StationMonitor (String dataType,String stationsTable,String redisNodes) {
        super(dataType, stationsTable, redisNodes);
    }

}
