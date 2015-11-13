#!/bin/bash

docker run \
	--name graph_hbase \
	-p 2181:2181 \
	-p 60010:60010 \
	-p 60000:60000 \
	-p 60020:60020 \
	-p 60030:60030 \
	-p 3306:3306 \
	-p 9000:9000 \
	--expose 3306 \
	--expose 9000 \
	-h default \
	-d \
	nerdammer/hbase:0.98.10.1
