# MapR Streams Internals Demo

This is a demo project to show the internals of MapR Streams. This project primarily has a Producer and Consumer
that can be executed in a mult-threaded manner.

Starting the Producer Command

java -Xmx7373m -cp \`mapr classpath\`:/tmp/uber-streams-internals-demo-0.1.jar com.mapr.sko.ps.StartSensors /sko/teststream:testtopic 1000 100 100 500 /tmp/data.json

StartSensors class starts multiple Producers based on the parameters passed in. Each producer will be running in its own thread

1) First Param - number of producers to start
2) Second Param - number of messages each producer should be sending. -1 implies keep sending messages until its killed
3) Third Param - number of milliseonds each producer should stop before sending another message
4) Fourth Param - size of the thread pool. If this is small than the first param then other threads will wait until the first batch is completed
5) Fifth Param - The json file to send. small-data.json is 170 bytes in size. data.json is 17 kb in size

So in the example above, there will be 1000 producers, each sending 100 messages and each waiting 100 ms before sending another message and
there are 500 thread pool so at a time only 500 threads are active.
