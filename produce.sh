#!/bin/bash

#export KAFKA_BROKERS="localhost:9092"
export KAFKA_BROKERS="localhost:29092,localhost:29093"
export KAFKA_TOPIC="input"
go run kafka_producer.go -n 5000000 -d 10
