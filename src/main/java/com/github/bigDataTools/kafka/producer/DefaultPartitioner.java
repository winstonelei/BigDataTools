package com.github.bigDataTools.kafka.producer;

import com.github.bigDataTools.kafka.model.DefaultMessage;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by winstone on 2017/6/12.
 */
public class DefaultPartitioner  implements Partitioner {


    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    public void configure(Map<String, ?> configs) {}


    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        int numPartitions = partitions.size();

        try {
            long partitionHash = ((DefaultMessage)value).getPartitionHash();
            //按hash分区
            if(partitionHash > 0){
                long index = partitionHash % numPartitions;
                //System.out.println("numPartitions:"+numPartitions+",partitionHash:"+partitionHash + ",index:"+index);
                return (int)index;
            }
        } catch (ClassCastException e) {}

        if (keyBytes == null) {
            int nextValue = counter.getAndIncrement();
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                int part = DefaultPartitioner.toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return DefaultPartitioner.toPositive(nextValue) % numPartitions;
            }
        } else {
            // hash the keyBytes to choose a partition
            return DefaultPartitioner.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    public void close() {}
}
