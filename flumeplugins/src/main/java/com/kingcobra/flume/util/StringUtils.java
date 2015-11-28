package com.kingcobra.flume.util;

/**
 * Created by kingcobra on 15/10/6.
 */
public class StringUtils {

    public static String replaceWhiteSpaceToComma(String content) {
        content = content.replaceAll("\\s+", ",");
        return content;
    }
}
