package com.kingcobra.flume.monitor;

import org.apache.flume.Event;

/**
 * Created by kingcobra on 15/9/28.
 * csv file parser
 * stationid is the first element
 */
public class CsvParser extends AbstractEventMonitor.EventParser {
    @Override
    public String parseEvent(Event event) {
        String lineContent = new String(event.getBody());
        String stationId = lineContent.split(",")[0];
        return stationId;
    }
}
