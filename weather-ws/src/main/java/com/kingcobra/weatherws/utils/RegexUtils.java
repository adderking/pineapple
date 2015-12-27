package com.kingcobra.weatherws.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/12/7.
 */
public class RegexUtils {
    /**
     * check the format of startTime and endTime in BusinessRule.
     * @param time
     * @return
     */
    public static boolean checkTimeInRule(String time) {
        Pattern pattern = Pattern.compile("now[+-]?\\d*");
        Matcher matcher = pattern.matcher(time);
        return matcher.matches();
    }
}
