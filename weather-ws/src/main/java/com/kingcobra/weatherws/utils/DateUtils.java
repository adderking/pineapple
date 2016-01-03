package com.kingcobra.weatherws.utils;

import com.google.common.base.Strings;
import com.kingcobra.weatherws.common.Constant;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/12/7.
 */
public class DateUtils {

    public static final DateFormat formatter = new SimpleDateFormat(Constant.DATEFORMAT);

    public static String parseDate(Date date) {
        return formatter.format(date);
    }

    public static Calendar parseDate(String date) {
        try {
            Date d = formatter.parse(date);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(d);
            return calendar;
        } catch (Exception e) {
            throw new RuntimeException("time style is error");
        }
    }

    /**
     * 计算周末，如果当前日期是本周六或者日，那么返回的是下周末的日期范围，这个日期范围是从周六零点到周一零点。
     * @param calendar
     * @return String数组, 0:星期六零时 1：星期一零时,日期格式为yyyyMMddHH
     */
    public static String[] calTimeScope(Calendar calendar) {
        Date[] weekend = new Date[2];
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);
        switch (dayWeek) {
            case Calendar.SATURDAY:
                calendar.add(Calendar.DATE,7);
                calendar.set(Calendar.HOUR_OF_DAY, 8);
                weekend[0] = calendar.getTime();break;
            case Calendar.SUNDAY:
                calendar.add(Calendar.DATE, 6);
                calendar.set(Calendar.HOUR_OF_DAY, 8);
                weekend[0] = calendar.getTime();break;
            default:
                calendar.add(Calendar.DATE,7-dayWeek);
                calendar.set(Calendar.HOUR_OF_DAY, 8);
                weekend[0] = calendar.getTime();
        }
        calendar.add(Calendar.DATE, 2);//得到周一零点的时间。
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        weekend[1] = calendar.getTime();
        String[] times = new String[2];
        times[0] = formatter.format(weekend[0]);
        times[1] = formatter.format(weekend[1]);
        return times;
    }
    /**
     * 返回起始结束日期的字符串格式
     * @param startTime 业务规则中时间范围的定义，格式now+1
     * @param endTime   业务规则中时间范围的定义，格式now+7
     * @return 格式为yyyyMMddHH的时间字符串数组
     */
    public static String[] calTimeScope(String startTime, String endTime) {
        Calendar startDay = parseTime(startTime);
        Calendar endDay = parseTime(endTime);
        int dayDiff = calDayDifference(startDay, endDay);
        char dayOrNight = calDayOrNight(startDay);
        //计算取值的时间范围，白天取当天8点至第二天早8点的数据，3小时或者12小时都适用。
        switch (dayOrNight) {
            case Constant.WEATHER_DAY:
                startDay.set(Calendar.HOUR_OF_DAY, 8);
                break;
            case Constant.WEATHER_NIGHT:
                int hour = startDay.get(Calendar.HOUR_OF_DAY);
                if (hour > 0 & hour < 8) {  //如果查询时间在第二天0点~8点之间，日期需要减1，从前一天21点开始取数据
                    startDay.add(Calendar.DAY_OF_MONTH,-1);
                    startDay.set(Calendar.HOUR_OF_DAY, 21);
                }else {
                    startDay.set(Calendar.HOUR_OF_DAY, 21);
                }
                break;
            default:
                startDay.set(Calendar.HOUR_OF_DAY, 8);
                break;
        }
        //计算取值的时间范围，夜间需要判断日期差值，如果只要今天的数据，那么判断白天还是夜间，白天的取值结束时间为晚8点，夜间的取值结束时间为第二天的晚21点
        //取值范围为右半开区间，如果时间范围大于一天，那么取值的结束时间为endDay代表的日期的23点。
        if (dayDiff == 1 & "now".equalsIgnoreCase(startTime)) {
            switch (dayOrNight) {
                case Constant.WEATHER_DAY:
                    endDay.set(Calendar.HOUR_OF_DAY, 8);
                    break;
                case Constant.WEATHER_NIGHT:
                    endDay.set(Calendar.HOUR_OF_DAY, 21);
                    break;
                default:
                    endDay.set(Calendar.HOUR_OF_DAY, 8);
                    break;
            }
        } else {
            endDay.set(Calendar.HOUR_OF_DAY, 23);
        }
        String s_startDay = formatter.format(startDay.getTime());
        String s_endDay = formatter.format(endDay.getTime());
        String[] dates = new String[]{s_startDay, s_endDay};
        return dates;
    }

    /**
     * 计算某个日期值是白天还是夜间,气象局的白天定义为早8点到晚20点之间,晚间定义为晚20点到第二天早晨8点之间。
     * @param date 符合yyyyMMddHH格式的日期字符串
     * @return  Constant.WEATHER_DAY or Constant.WEATHER_NIGHT
     */
    public static char calDayOrNight(String date) {
        Calendar time = parseDate(date);
        return calDayOrNight(time);
    }

    public static char calDayOrNight(Calendar date) {
        int h = date.get(Calendar.HOUR_OF_DAY);
        if (h >= 8 & h < 18) {
            return Constant.WEATHER_DAY;
        }else {
            return Constant.WEATHER_NIGHT;
        }
    }
    /**
     * 计算两个时间点的天数差值
     * @param startdate
     * @param endDate
     * @return endDate与startDate差几天
     */
    private static int calDayDifference(Calendar startdate, Calendar endDate) {
        try {
            int start_dayOFYear = startdate.get(Calendar.DAY_OF_YEAR);
            int end_dayOFYear = endDate.get(Calendar.DAY_OF_YEAR);
            return end_dayOFYear - start_dayOFYear;
        } catch (Exception e) {
            return 0;
        }
    }



    /**
     * 使用正则表达式解析时间
     * @param time 业务规则中的时间范围 now ,now+N
     * @return
     */
    private static Calendar parseTime(String time) {
        Pattern pattern = Pattern.compile("(now)([+-]{0,1})(\\d{0,2})");
        Matcher matcher = pattern.matcher(time);
        matcher.matches();
        Calendar calendar = Calendar.getInstance();
        //测试用
       /* calendar.set(Calendar.MONTH, 6);
        calendar.set(Calendar.DAY_OF_MONTH, 31);
        calendar.set(Calendar.HOUR_OF_DAY, 5);
        calendar.getTime();*/


        String operator = matcher.group(2);

        if (!Strings.isNullOrEmpty(operator)) {
            int startTimeLevel = Integer.valueOf(matcher.group(3));
            if ("+".equals(operator)) {
                calendar.add(Calendar.DATE, startTimeLevel);
            } else if ("-".equals(operator)) {
                calendar.add(Calendar.DATE, -startTimeLevel);
            }
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
        }
        return calendar;
    }


}
