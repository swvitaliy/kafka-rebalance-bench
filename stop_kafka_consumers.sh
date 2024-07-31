#!/bin/bash

ps aux | fgrep kafka_consumer | fgrep group1 | awk '{print $2;}' | xargs kill -9