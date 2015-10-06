package com.kingcobra.flume.monitor;

import com.kingcobra.kedis.core.RedisConnector;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.util.List;

/**
 * Created by kingcobra on 15/9/22.
 */
public abstract class AbstractEventMonitor implements EventMonitor {
    protected RedisConnector redisConnector;
    private String redisNodes;
    protected EventParser parseEvent;

    public AbstractEventMonitor(String redisNodes) {
        this.redisNodes = redisNodes;
    }

    @Override
    public void eventMonitored(Event event) {
        this.parseEvent.parseEvent(event);
    }
    @Override
    public void eventsMonitored(List<Event> events) {
        if(events.isEmpty())
            return;
        for (org.apache.flume.Event event : events) {
            eventMonitored(event);
        }
    }
    @Override
    public void initialize(EventParser parseEvent) {
        redisConnector = RedisConnector.Builder.build(redisNodes, Constant.REDIS_DEFAULT_MAXTOTAL,Constant.REDIS_DEFAULT_MAXIDEL,Constant.REDIS_DEFAULT_MAXWAITMILLIS);
        this.parseEvent  = parseEvent;
    }

    @Override
    public void close() {
        if(redisConnector!=null)
            redisConnector.closeJedisCluster();
    }

    public static abstract class EventParser {
        protected Context ctx;
        public void setContext(Context ctx){
            this.ctx = ctx;
        }
        public abstract String parseEvent(Event event);
    }
}
