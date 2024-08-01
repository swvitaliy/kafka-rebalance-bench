#!/bin/bash

#export KAFKA_BROKERS="localhost:9092"
export KAFKA_BROKERS="localhost:29092,localhost:29093"

go build -o kafka_consumer kafka_consumer.go

echo "Start kafka consumer" > output.txt
for i in {1..100}; do
  ./kafka_consumer -topic input --delay 500 --group=group1 &>> output.txt &
done

