package com.kingcobra.kedis.core;

import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by kingcobra on 15/8/13.
 */
public class Constant {
    public static final Set<HostAndPort> RMASTER_HOST_AND_PORT = new HashSet<>();
    static{
       /* RMASTER_HOST_AND_PORT.add(new HostAndPort("192.168.1.112", 6378));
        RMASTER_HOST_AND_PORT.add(new HostAndPort("192.168.1.112", 6379));
        RMASTER_HOST_AND_PORT.add(new HostAndPort("192.168.1.114", 6378));*/
        RMASTER_HOST_AND_PORT.add(new HostAndPort("192.168.1.109", 6378));
        RMASTER_HOST_AND_PORT.add(new HostAndPort("192.168.1.110", 6378));
        RMASTER_HOST_AND_PORT.add(new HostAndPort("192.168.1.111", 6378));
    }
}
