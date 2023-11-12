#!/bin/bash

for cont in $(docker container ls -a -f name='^chord_chordNode*'); do
    docker container rm $cont 2> /dev/null
done

export NET_NAME="chord-net"
export CONTAINER_ID=$(basename $(cat /proc/1/cpuset))  
export NODE_REPLICAS=32
export PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}/opt/chord/init_node/ChordNodeCode/"

sudo find /var/lib/docker/containers/ -type f -name "*.log" -delete