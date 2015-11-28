package com.kingcobra.flume.kafkapartitioner;

import kafka.producer.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kingcobra on 15/10/13.
 */
public class MultiPartitioner implements Partitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiPartitioner.class);

    public MultiPartitioner(kafka.utils.VerifiableProperties verifiableProperties) {

    }
    @Override
    public int partition(Object key, int numPartitions) {
        int _key,partitionIndex;
        try {
            _key = Integer.parseInt((String) key);
            partitionIndex=Math.abs(_key % numPartitions);
            return partitionIndex;
        } catch (Exception e) {
            partitionIndex=Math.abs(key.hashCode() % numPartitions);
            return partitionIndex;
        }
    }
}
