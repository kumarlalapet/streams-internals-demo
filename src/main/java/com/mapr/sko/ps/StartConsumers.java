package com.mapr.sko.ps;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by mlalapet on 3/8/17.
 */
public class StartConsumers {

    public static void main(String args[]) throws FileNotFoundException {

        if(args.length < 7) {
            System.err.println("Arguments: topic group_id client_id by-topic(true|false) no-of-threads thread-pool-size");
            System.exit(0);
        }

        String topic = args[0];
        String grpId = args[1];
        String clId = args[2];
        boolean byTopic = Boolean.valueOf(args[3]);
        int noOfThreads = Integer.parseInt(args[4]);
        int threadPoolSize = Integer.parseInt(args[5]);
        boolean consumerByPartition = Boolean.valueOf(args[6]);

        ExecutorService consumerPool = Executors.newFixedThreadPool(threadPoolSize);

        for(int i =0; i < noOfThreads; i++) {
            String lgrpId = byTopic == true ? grpId+(i+1) : grpId;
            String lclId = byTopic == true ? clId : clId+(i+1);
            String localTopic = byTopic == true ? topic+(i+1) : topic;
            consumerPool.submit(new SensorMessageConsumer(localTopic,lgrpId,lclId,(i+1),consumerByPartition));
        }
    }
}
