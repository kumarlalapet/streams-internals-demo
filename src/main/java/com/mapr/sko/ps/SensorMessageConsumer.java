package com.mapr.sko.ps;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by mlalapet on 5/5/16.
 */
public class SensorMessageConsumer implements Runnable {

    private final String topic;
    private final String grpId;
    private final String clId;
    private int partitionId;
    private boolean consByPart;
    private KafkaConsumer consumer;

    public SensorMessageConsumer(String topic, String grpId, String clId, int partitionId, boolean consByPart) {
        this.topic = topic;
        this.grpId = grpId;
        this.clId = clId;
        this.partitionId = partitionId;
        this.consByPart = consByPart;
        consumer = configureConsumer();
    }

    @Override
    public void run() {

        // Subscribe to the topic.
        System.out.println("AAAAA  "+topic+" "+grpId+" "+clId+" "+partitionId+" "+consByPart);

        // Set the timeout interval for requests for unread messages.
        long pollTimeOut = 3000;
        if(consByPart) {
            List<TopicPartition> topics = new ArrayList();
            topics.add(new TopicPartition(topic, partitionId));
            consumer.assign(topics);
        } else {
            List<String> topics = new ArrayList();
            topics.add(topic);
            consumer.subscribe(topics, new RebalanceListener());
        }

        try {
            do {
                // Request unread messages from the topic.

                ConsumerRecords<String, String>     consumerRecords = consumer.poll(pollTimeOut);

                Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
                if (iterator.hasNext()) {
                    while (iterator.hasNext()) {
                        ConsumerRecord<String, String> record = iterator.next();

                        if (processRecords(record)) {
                            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                            offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1));
                            consumer.commitAsync(offsets, new OffsetCommitCallback() {
                                @Override
                                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                                    //Any after commit work can be done here
                                }
                            });

                            String message = record.toString();
                            System.out.println((" Consumed Record: " + message));

                        }

                    }
                }

            } while (true);
        } finally {
            consumer.close();
        }
    }

    private static boolean processRecords(ConsumerRecord<String, String> record) {
        return true;
    }

    class RebalanceListener implements ConsumerRebalanceListener {

        public RebalanceListener() {
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for(TopicPartition partition: partitions) {
                offsets.put(partition, new OffsetAndMetadata(consumer.position(partition)) );
            }
            consumer.commitSync(offsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for(TopicPartition partition: partitions) {
                consumer.seek(partition, consumer.position(partition));
            }
        }
    }

    /* Set the value for a configuration parameter.
       This configuration parameter specifies which class
       to use to deserialize the value of each message.*/
    public KafkaConsumer configureConsumer() {
        Properties props = new Properties();
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", this.grpId);
        props.put("client.id", this.clId);
        props.put("enable.auto.commit", "false");
        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }

}
