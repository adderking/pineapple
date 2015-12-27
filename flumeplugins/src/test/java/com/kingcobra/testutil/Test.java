package com.kingcobra.testutil;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.kingcobra.flume.util.StringBloomFilter;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by kingcobra on 15/9/15.
 */
public class Test {
    @org.junit.Test
    public void testBloomFilter() {
        StringBloomFilter stringBloomFilter = new StringBloomFilter(500000, 0.01d);
        for (int i = 10000; i < 99999; i++) {
            stringBloomFilter.put(Integer.toString(i));
        }
        stringBloomFilter.put("54511");
        Set<String> stations = new HashSet<String>();
        stations.add("54511");
        stations.add("64511");
        stations.add("24511");
        stations.add("84511");
        stations.add("768321");
        for (String s : stations) {
            if (stringBloomFilter.isContain(s)) {
                System.out.println(s + " is exist");
            } else {
                System.out.println(s + " is not exist");
            }
        }
    }
    @org.junit.Test
    public void testClassLoader() throws ClassNotFoundException,InstantiationException,IllegalAccessException{
        Class clazz = Class.forName("com.kingcobra.flume.interceptor.PmscInterceptor$Builder");
        Interceptor.Builder builder = (Interceptor.Builder)clazz.newInstance();
        System.out.println(builder.toString());
    }

}
