#!/bin/bash

docker run --name graph --net container:graph_hbase -d s2graph:0.12.0-SNAPSHOT
