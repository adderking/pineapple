package com.kingcobra.flume.monitor;

import org.apache.flume.Event;

import java.util.List;

/**
 * Created by kingcobra on 15/9/22.
 */
public interface EventMonitor{
    /**
     * single event handled
     * @param event
     */

    public void eventMonitored(Event event);

    /**
     * 监控结果计算
     */
    public void monitorResult();

    /**
     * initialize eventparser
     * @param parseEvent
     */
    public void initialize(AbstractEventMonitor.EventParser parseEvent);

    /**
     * multi events handled
     * @param events
     */
    public void eventsMonitored(List<Event> events);

    /**
     * reset monitor cache
     */
    public void resetMonitor();

    /**
     * close connection when flume source closed
     */
    public void close();

    /**
     * define monitor config items in flume configuration file
     */
    public class Constant{
        //prefix is "monitor." like "monitor.key"
        public static final String MONITOR_DATATYPE = "dataType";
        public static final String MONITOR_STATIONS = "stations";
        public static final String MONITOR_REDIS = "redisnodes";
        public static final String REDIS_DEFAULT_MAXTOTAL ="10000";
        public static final String REDIS_DEFAULT_MAXIDEL = "2000";
        public static final String REDIS_DEFAULT_MAXWAITMILLIS ="10000";

        public static final Integer BLOOMFILTER_EXPECTINSERTIONS =500000;
        public static final Double BLOOMFILTER_POSITIVE =0.01;
    }
}
