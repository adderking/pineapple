package com.kingcobra.flume.util;


import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * Created by kingcobra on 15/9/15.
 */
public class StringBloomFilter {
    private BloomFilter<CharSequence> bloomFilter ;

    public StringBloomFilter(int expectInsertions,double positive) {
        this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(), expectInsertions, positive);
    }

    public boolean isContain(String stationId) {
        return bloomFilter.mightContain(stationId);
    }

    public void put(String stationId) {
        bloomFilter.put(stationId);
    }
}
