#!/bin/bash

java -cp target/sko-kafka-1.0-SNAPSHOT.jar org.apache.kafka.clients.tools.ProducerPerformance sko-storm-demo 100000 500
