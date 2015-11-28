package com.kingcobra.flume.monitor;

import com.google.common.base.Charsets;
import com.kingcobra.flume.util.StringUtils;
import org.apache.flume.Event;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/10/4.
 */
public class PmscParser extends AbstractEventMonitor.EventParser {
    private static final Pattern pattern = Pattern.compile(Constant.STATIONID_REGX,Pattern.DOTALL);
    private Matcher matcher;
    @Override
    public String parseEvent(Event event) {
        String content = new String(event.getBody(), Charsets.UTF_8).trim();
        content = StringUtils.replaceWhiteSpaceToComma(content);
        matcher = pattern.matcher(content);
        boolean isMatch = matcher.matches();
        String stationId =null;
        if (isMatch) {
            stationId = matcher.group(1);
        }
        return stationId;
    }
    public class Constant{
        //stationID    lon    lat    height    UTC    LST    report_time(LST)   GMT+8    times_num
        public static final String STATIONID_REGX ="(\\d+),(\\-?\\d+\\.\\d*),(\\-?\\d+\\.\\d*),(\\-?\\d+\\.\\d*),(\\d+),(\\d+),(\\-?\\d+\\.\\d*),(\\d+),(\\d+),(\\d+)";
        //dismiss row
        public static final String OVERLOOK_REGX = "^[\\D]+.*";
    }

}
