# MATI-420808-Taller3-RealTime
MATI-420808-Taller3-RealTime


## Prerequisites

 config.vm.network "forwarded_port", guest: 18080, host: 18080
 
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

export SPARK_WORKER_INSTANCES="3"
export SPARK_WORKER_CORES="1"

Para correr un solo worker
sbin/start-all.sh

Para correr varios workers
./start-master.sh
./start-slave spark://vagrant-ubuntu-trusty-64:7077

##Run the project

`GenericRealTimeProcessor localhost:9092 temperature localhost:7077 54.191.40.134 27017 meteor temperatura`

broker1-host:port topic1 spark-master-node-host:port mongo-server-ip mongo-server-port database collection