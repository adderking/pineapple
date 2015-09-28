package com.kingcobra.flume.monitor;

import org.apache.flume.Event;

import java.util.List;

/**
 * Created by kingcobra on 15/9/22.
 */
public interface EventMonitor {
    public void readEvent(Event event);

    public void stationsDiff();

    public void initialize(AbstractEventMonitor.EventParser parseEvent);

    public void readEvents(List<Event> events);

    public void refreshBloomFilter();

    public void close();

    public class Constant{
        //prefix is "monitor." like "monitor.key"
        public static final String MONITOR_KEY = "key";
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
