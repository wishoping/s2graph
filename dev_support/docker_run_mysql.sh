#!/bin/bash

docker run \
	--name graph_mysql \
	-e MYSQL_ROOT_PASSWORD=graph \
	-p 3306:3306\
	-d \
	graph_mysql

