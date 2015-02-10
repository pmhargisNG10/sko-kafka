#!/bin/bash

java -cp target/sko-kafka-1.0-SNAPSHOT.jar org.apache.kafka.clients.tools.ProducerPerformance hdppmh1-master-01.cloudapp.net:9092 ../dts_data/100000lines.tsv storm-demo 100000 500 

##java -cp target/sko-kafka-1.0-SNAPSHOT.jar org.apache.kafka.clients.tools.ProducerPerformance hdppmh1-master-01.cloudapp.net:9092 ../dts_data/100000lines.tsv storm-demo 500 10 batch.size=10
