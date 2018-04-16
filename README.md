# MATI-420808-Taller3-RealTime
MATI-420808-Taller3-RealTime


## Prerequisites

Verify the spark version. On your server execute the following command
spark-submit --version
```
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.0
      /_/

```

Verify the Kafka server.properties file
advertised.listeners=PLAINTEXT://localhost:9092

Run spark master and slaves nodes
sbin/start-all.sh

##Run the project

`GenericRealTimeProcessor localhost:9092 temperature localhost:7077 54.191.40.134 27017 meteor temperatura`

broker1-host:port topic1 spark-master-node-host:port mongo-server-ip mongo-server-port database collection