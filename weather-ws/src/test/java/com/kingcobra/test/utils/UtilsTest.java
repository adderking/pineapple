package com.kingcobra.test.utils;

import com.alibaba.fastjson.JSONArray;
import com.kingcobra.test.MyAppConfig;
import com.kingcobra.weatherws.WebConfig;
import com.kingcobra.weatherws.common.AreaHelper;
import com.kingcobra.weatherws.utils.DateUtils;
import com.kingcobra.weatherws.utils.RegexUtils;
import com.kingcobra.weatherws.utils.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by kingcobra on 15/12/8.
 */
/*@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextHierarchy({
        @ContextConfiguration(classes = WebConfig.class),
        @ContextConfiguration(classes = MyAppConfig.class)
})*/
public class UtilsTest {

    @Autowired(required=true)
    private AreaHelper areaHelper;


    @Test
    public void testGetWeekend() {
        String d = "20151213";
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        try {
            Date date = df.parse(d);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            String[] dates = DateUtils.calTimeScope(calendar);
            System.out.println(dates[0]);
            System.out.println(dates[1]);
        }catch(Exception e){
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFindInternalAreaStations() {
        String areaId = "101010100";
        JSONArray aroundAreas = areaHelper.findAroundArea("tableName", areaId);
        Assert.assertTrue(aroundAreas.size() == 0);

    }

    @Test
    public void testDateUtils() {
        String startTime = "now+20";
        String endTime = "now+7";
       boolean isRight=  RegexUtils.checkTimeInRule(startTime);
        System.out.println(isRight);
        String[] dates=DateUtils.calTimeScope(startTime, endTime);
        System.out.println(dates[0]);
        System.out.println(dates[1]);
        Calendar calendar = Calendar.getInstance();
        int hour = calendar.get(Calendar.HOUR);
        int hour_in_day = calendar.get(Calendar.HOUR_OF_DAY);
        System.out.println(hour + "," + hour_in_day);
    }
    @Test
    public void testStringUtils() {
        String d = "1.0";
        DecimalFormat format = new DecimalFormat("00");
        String s = format.format(Double.valueOf(d));
        System.out.println(s);

    }
    @Test
    public void testPar() {
        testParam();
    }
    private void testParam(String... columns) {
        System.out.println(columns == null);
        System.out.println(columns.length == 0);
    }

}
