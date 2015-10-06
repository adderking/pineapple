package com.kingcobra.flume.monitor;

import com.kingcobra.flume.util.StringUtils;
import org.apache.flume.Event;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/10/4.
 */
public class PmscParser extends AbstractEventMonitor.EventParser {
    private static final String EVENTREGX ="(\\d+),(\\d+\\.\\d*),(\\d+\\.\\d*),(\\d+\\.\\d*),(\\d+),(\\d+),(\\d+\\.\\d*),(\\d+),(\\d+),(\\d+)";
//    private static final String EVENTREGX ="(\\d+)\\s+(\\d+\\.\\d*)\\s+(\\d+\\.\\d*)\\s+(\\d+\\.\\d*)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+\\.\\d*)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)";
    private static final Pattern pattern = Pattern.compile(EVENTREGX);
    private Matcher matcher;
    @Override
    public String parseEvent(Event event) {
        String content = new String(event.getBody()).trim();
        content = StringUtils.replaceWhiteSpaceToComma(content);
        matcher = pattern.matcher(content);
        boolean isMatch = matcher.matches();
        String stationId =null;
        if (isMatch) {
            stationId = matcher.group(1);
        }
        return stationId;
    }
}
