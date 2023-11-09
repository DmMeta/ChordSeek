#!/bin/bash

NET_NAME="chord-net"
NET_OPTIONS="--driver=bridge --subnet=10.0.0.0/25 --gateway=10.0.0.1 --ip-range=10.0.0.2/26"

CONTAINER_ID=$(basename $(cat /proc/1/cpuset))  
CHORD_DATA_VOLUME="ChordNodeData"

EXISTING_NET=$(docker network ls | grep $NET_NAME)
export NODE_REPLICAS=32

if [ -z "$EXISTING_NET" ]; then
    echo "The network with name: '$NET_NAME' does not exist.Creating..."
    docker network create $NET_OPTIONS $NET_NAME 
    docker network connect --ip=10.0.0.2 $NET_NAME $CONTAINER_ID
else
     echo "The network with name: '$NET_NAME' already exists. Proceeding..."
     if [[ ! $(docker network inspect "$NET_NAME") == *"$CONTAINER_ID"* ]]; then
        docker network connect --ip=10.0.0.2 $NET_NAME $CONTAINER_ID
     fi     
fi

if ! docker volume inspect $CHORD_DATA_VOLUME &> /dev/null; then
    echo "Creating volume $CHORD_DATA_VOLUME"
    docker volume create --name $CHORD_DATA_VOLUME
fi

docker compose -f init_node/compose.yml -p chord up  


