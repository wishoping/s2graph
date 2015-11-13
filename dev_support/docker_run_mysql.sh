#!/bin/bash

docker run \
	--name graph_mysql \
	-e MYSQL_ROOT_PASSWORD=graph \
	--net container:graph_hbase \
	-d \
	graph_mysql

