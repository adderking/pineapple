package com.kingcobra.flume;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.kingcobra.flume.interceptor.PmscInterceptor;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * Created by kingcobra on 15/10/7.
 */
public class TestPmscInterceptor {
    private List<String> data = Lists.newLinkedList();
    private List<Event> events ;
    @Before
    public void init() {
        data.add("OCF_LST_3H  20076 2015073100(UTC)    57   11   999.9");
        data.add("stationID    lon    lat    height    UTC    LST    report_time(LST)   GMT+8    times_num");
        data.add("TIME_STEP    UTC   LST   TEMP   TEMP_MAX   TEMP_MIN   RAIN     FF    FF_LEVEL   DD   DD_LEVEL   CLOUD   WEATHER   RH");
        data.add("    3772   359.5    51.4    24.0    2015073100  2015073100    0.0  2015073020  2015073108   57");
        data.add(" 00   -03   2015073021    14.0    16.0    11.0     0.1     1.7     0.0   304.0     7.0    69.7     7.0    72.2");
        data.add(" 03    00   2015073100    10.0    12.0    10.0     0.0     2.1     0.0     2.2     8.0    60.2     1.0    83.4");
        data.add("OCF_LST_3H  20076 2015073100(UTC)    57   11   999.9");
        data.add("stationID    lon    lat    height    UTC    LST    report_time(LST)   GMT+8    times_num");
        data.add("TIME_STEP    UTC   LST   TEMP   TEMP_MAX   TEMP_MIN   RAIN     FF    FF_LEVEL   DD   DD_LEVEL   CLOUD   WEATHER   RH");
        data.add("    3166   359.5    51.4    24.0    2015073100  2015073100    0.0  2015073020  2015073108   57");
        data.add(" 06    03   2015073103     8.0    10.0     8.0     0.0     1.8     0.0     6.0     8.0    68.1     1.0    91.5");
        data.add("357   354   2015081418    14.0    20.0    14.0     3.9     6.2     1.0   245.4     5.0    75.5   301.0    89.5");
        events = Lists.newArrayListWithCapacity(data.size());
        for (String s : data) {
            Event event = new SimpleEvent();
            event.setBody(s.getBytes());
            events.add(event);
        }
    }

    @Test
    public void testInterceptor() {
        try {
            Interceptor.Builder build = InterceptorBuilderFactory.newInstance("com.kingcobra.flume.interceptor.PmscInterceptor$Builder");
            Interceptor interceptor = build.build();
            interceptor.initialize();
            List<Event> result = interceptor.intercept(events);
            for (Event event : result) {
                String content = new String(event.getBody());
                System.out.println(content);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
