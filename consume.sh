#!/bin/bash

export KAFKA_BROKERS="localhost:9092"

go build -o kafka_consumer kafka_consumer.go

echo "Start kafka consumer" > output.txt
for i in {1..250}; do
  ./kafka_consumer -topic input --delay 500 --group=group1 &>> output.txt &
done

