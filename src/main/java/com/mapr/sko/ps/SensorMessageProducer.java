package com.mapr.sko.ps;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.security.SecureRandom;
import java.util.Properties;

/**
 * Created by mlalapet on 3/1/17.
 */
public class SensorMessageProducer implements Runnable {
    private static String msg;
    private SecureRandom random = new SecureRandom();

    private final String topic;
    private final String sensorId;
    private final int noOfMessagesToSend;
    private final long sleepTime;
    //private final boolean smallMsg;
    private int sensorNumber;

    public SensorMessageProducer(String topic, String sensorId, int noOfMessagesToSend,
                                 long sleepTime, int sensorNumber) {
        this.topic = topic;
        this.sensorId = sensorId;
        this.noOfMessagesToSend = noOfMessagesToSend;
        this.sleepTime = sleepTime;
        //this.smallMsg = smallMsg;
        this.sensorNumber = sensorNumber;
    }

    @Override
    public void run() {

        long ct1 = System.currentTimeMillis();
        System.out.println("Snesor id "+sensorNumber+ " started ");

        KafkaProducer producer = createProducer();
        try{
            int count = 0;
            boolean sendMsg = noOfMessagesToSend > 0 ? true : (noOfMessagesToSend == -1 ? true : false);
            while(sendMsg) {

                ProducerRecord rec = new ProducerRecord(topic, sensorId, msg);
                producer.send(rec);
                count = count + 1;

                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if(noOfMessagesToSend != -1) {
                    if(count == noOfMessagesToSend)
                        sendMsg = false;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.flush();
        producer.close();
        long ct2 = System.currentTimeMillis();
        System.out.println("Snesor id "+sensorNumber+ " took "+(ct2-ct1));
    }

    private KafkaProducer createProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(props);
    }

    public static void generateMessage(String jsonFile) throws FileNotFoundException {

        //if(!smallMsg) {
            Gson gson = new Gson();
            SensorLargeMessage msgObj = gson.fromJson(new FileReader(jsonFile), SensorLargeMessage.class);
            msg = gson.toJson(msgObj);
            gson = null;
        //} else {
        //    msg = "Im alive !............and sending a short message...";
        //}
    }

    private static class SensorLargeMessage {
        private SensorData[] array;
    }

    private static class SensorData {
        private String nm;
        private String cty;
        private String hse;
        private String yrs;
    }
}
