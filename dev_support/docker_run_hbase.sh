#!/bin/bash

docker run --name graph_hbase -h docker-box -p 2181:2181 -p 60010:60010 -p 60000:60000 -p 60020:60020 -p 60030:60030 -d nerdammer/hbase:0.98.10.1