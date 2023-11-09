#!/bin/bash

export NET_NAME="chord-net"
NET_OPTIONS="--driver=bridge --subnet=10.0.0.0/25 --gateway=10.0.0.1 --ip-range=10.0.0.2/26"

export CONTAINER_ID=$(basename $(cat /proc/1/cpuset))  
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

echo "Compiling code for stubs and protobuffer definitions..."
python3 -m grpc_tools.protoc -I ./init_node/protobufs --python_out=./init_node/ChordNodeCode \
        --grpc_python_out=./init_node/ChordNodeCode ./init_node/protobufs/chordprot.proto

if [ $? -eq 0 ]; then
    echo "Compilation was successfull!"
else
    echo "Compilation failed"
fi

docker compose -f init_node/compose.yml -p chord up  
# docker compose -p chord down --> to bring down havoc.

