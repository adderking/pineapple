package com.kingcobra.flume;

import com.kingcobra.flume.monitor.AbstractEventMonitor;
import com.kingcobra.flume.monitor.StationMonitor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kingcobra on 15/9/28.
 */
public class TestMonitor {
    private Context ctx ;
    private List<Event> events;
    @Before
    public void init() {
        ctx = new Context();
        ctx.put("dataType","forecast3h.external");
        ctx.put("stations", "external.stations");
        ctx.put("redisnodes", "[\"192.168.1.112:6378\",\"192.168.1.113:6378\",\"192.168.1.114:6378\",\"192.168.1.112:6379\",\"192.168.1.113:6379\",\"192.168.1.114:6379\"]");
        ctx.put("type", "com.kingcobra.flume.monitor.CsvParser");
        events = new ArrayList<Event>();
        for (int i = 0; i < 5; i++) {
            Event event = new SimpleEvent();
            event.setBody((51511+i+",12313,fasfsf").getBytes());
            events.add(event);

        }



    }

    @Test
    public void testCsvParser() {
        String parseType = ctx.getString("type");
        try {
            Class clazz = Class.forName(parseType);
            AbstractEventMonitor.EventParser parseEvent = (AbstractEventMonitor.EventParser)clazz.newInstance();
            parseEvent.setContext(ctx);
            StationMonitor stationMonitor = new StationMonitor(ctx);
            stationMonitor.initialize(parseEvent);
            stationMonitor.readEvents(events);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

    }
}
