package com.mapr.sko.ps;

import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by mlalapet on 3/1/17.
 */
public class StartSensors {

    public static void main(String args[]) throws FileNotFoundException {

        if(args.length < 6) {
            System.err.println("Arguments: topic number_of_sensors number_of_msg_per_sensor sleep_time jsonfile(if-small_msg-is-true)");
            System.exit(0);
        }

        SecureRandom random = new SecureRandom();

        String topic = args[0];
        int noOfSensors = Integer.parseInt(args[1]);
        int noOfMessagePerSensor = Integer.parseInt(args[2]);
        long sleepTime = Long.parseLong(args[3]);
        int threadPoolSize = Integer.parseInt(args[4]);
        //boolean smallMsg = false;//Boolean.valueOf(args[5]);
        String jsonFile = "";
        //if(!smallMsg) {
            jsonFile = args[5];
        //}
        boolean topicPerSensor = false;
        if(args.length == 7)
            topicPerSensor = Boolean.valueOf(args[6]);
        SensorMessageProducer.generateMessage(jsonFile);

        //List<Future> futures = new ArrayList<>();
        //List<Future> doneFutures = new ArrayList<>();
        ExecutorService sensorsPool = Executors.newFixedThreadPool(threadPoolSize);//(noOfSensors);
        for(int i =0; i < noOfSensors; i++) {
            String sensorId = new BigInteger(130, random).toString(32);
            String localTopic = topicPerSensor == true ? topic+(i+1) : topic;
            Future f = sensorsPool.submit(new SensorMessageProducer(localTopic,sensorId,noOfMessagePerSensor,sleepTime, (i+1)));
            //futures.add(f);
            //System.out.println(i);
        }

/**
        boolean stop = false;
        int count = 0;
        while(!stop) {
            for(Future f : futures) {
                if(f.isDone()) {
                    f.cancel(true);
                    doneFutures.add(f);
                    count = count + 1;
                    //System.out.println(count);
                }
            }
            for(Future f : doneFutures)
                futures.remove(f);
            doneFutures.clear();
            if(futures.size() == 0)
                stop = true;
        }

        System.exit(0); **/
    }

}
