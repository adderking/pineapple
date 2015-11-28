package com.kingcobra.flume.interceptor;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.kingcobra.flume.monitor.PmscParser;
import com.kingcobra.flume.util.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/9/13.
 *
 */
public class PmscInterceptor implements Interceptor {
    private String stationInfo ;
    private Pattern stationid_regx_pattern;
    private Pattern overlook_regx_pattern;
    private Matcher stationMatcher;
    private Matcher overlookMatcher;
    private static final Logger LOGGER = LoggerFactory.getLogger(PmscInterceptor.class);
    @Override
    public void initialize() {
        stationInfo = null;
        stationid_regx_pattern = Pattern.compile(PmscParser.Constant.STATIONID_REGX,Pattern.DOTALL);
        overlook_regx_pattern = Pattern.compile(PmscParser.Constant.OVERLOOK_REGX,Pattern.DOTALL);
    }

    @Override
    public Event intercept(Event event) {
        String content = new String(event.getBody(), Charsets.UTF_8).trim();
        overlookMatcher = overlook_regx_pattern.matcher(content);
        if (overlookMatcher.matches()) {
            stationInfo=null;
            return null;
        }
        content = StringUtils.replaceWhiteSpaceToComma(content);
        stationMatcher = stationid_regx_pattern.matcher(content);
        if (stationMatcher.matches()) {
            stationInfo = content+ ",";
            return null;
        }else if (stationInfo != null) {
            event.setBody((stationInfo + content).getBytes(Charsets.UTF_8));
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> newEventList = Lists.newArrayListWithCapacity(list.size());
        Event _e=null;
        for (Event event : list) {
            _e=intercept(event);
            if (_e != null) {
                newEventList.add(_e);
            }
        }
        return newEventList;
    }

    @Override
    public void close() {
        //no...OP
    }
    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new PmscInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
