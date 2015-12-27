package com.kingcobra.weatherws.utils;

import java.text.DecimalFormat;

/**
 * Created by kingcobra on 15/12/16.
 */
public class StringUtils {
    /**
     * 将数字中的小数点去掉，格式化为整形数字形式,少于两位的前面用0补位
     * @param numeric
     * @return
     */
    public static String formatNumeric(String numeric,String pattern) {
        DecimalFormat format = new DecimalFormat(pattern);
        String s = format.format(Double.valueOf(numeric));
        return s;
    }

    /**
     * 判断给定字符串是否为空
     * @param s
     * @return
     */
    public static boolean isNullOrEmpty(String s) {
        if (s == null || s.length() == 0) {
            return true;
        } else {
            return false;
        }

    }

    /**
     * 判断字符串是否非空
     * @param s
     * @return
     */
    public static boolean isNotNullAndEmpty(String s) {
        return !isNullOrEmpty(s);
    }
}
