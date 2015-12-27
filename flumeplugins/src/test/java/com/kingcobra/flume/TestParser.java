package com.kingcobra.flume;

import com.kingcobra.flume.monitor.AbstractEventMonitor;
import com.kingcobra.flume.monitor.PmscParser;
import com.kingcobra.flume.monitor.StationMonitor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/10/5.
 */
public class TestParser {
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
            stationMonitor.eventsMonitored(events);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testPmscParser() {
        File file = new File("/Users/kingcobra/Documents/PMSC_FINE_3H_15DAY_201507311200.DAT");
        try {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String content,stationId;
            while ((content = bufferedReader.readLine()) != null) {
                Event event = new SimpleEvent();
                event.setBody(content.getBytes());
                PmscParser pmscParser = new PmscParser();
                stationId= pmscParser.parseEvent(event);
                if (stationId != null) {
                    System.out.println(stationId);
                }
            }
            bufferedReader.close();
            fileReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPmscOverLookRegx() {
        String[] content = new String[2];
        content[0] = "TIME_STEP    UTC   LST   TEMP   TEMP_MAX   TEMP_MIN   RAIN     FF    FF_LEVEL   DD   DD_LEVEL   CLOUD   WEATHER   RH";
        content[1] = "00   2015073021    14.0";
        String regx = "^[\\D]+.*|^(\\d+)\\s+(\\d+)\\s+(\\d+\\.*\\d+)";
        for (String c : content) {
            Pattern pattern = Pattern.compile(regx);
            Matcher matcher = pattern.matcher(c);
            boolean isMatches = matcher.matches();
            System.out.println(isMatches);
        }
    }
}
