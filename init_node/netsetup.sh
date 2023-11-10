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

if [ -z $(find ./init_node/ChordNodeCode -iname "*_pb2_*.py") ]; then
    echo "Protobuffer and stubs are not compiled! Compiling code for stubs and protobuffer definitions..."
    python3 -m grpc_tools.protoc -I ./init_node/protobufs --python_out=./init_node/ChordNodeCode \
            --grpc_python_out=./init_node/ChordNodeCode ./init_node/protobufs/chordprot.proto
    if [ $? -eq 0 ]; then
        echo "Compilation was successfull!"
        echo "Changing import in generated stub code for relative imports to work"
        sed -i 's/import chordprot_pb2 as chordprot__pb2/from . import chordprot_pb2 as chordprot__pb2/' ./init_node/ChordNodeCode/chordprot_pb2_grpc.py
    else
        echo "Compilation failed"
    fi
else 
echo "Protobuffer and stubs are already compiled! Skipping..."
fi

if [ -z "$(docker container ls -a  --format '{{.Names}}' | grep -E '^chord_chordNode')" ]; then
  echo "Nodes are non-existent. Creating and starting Nodes..." 
  docker compose -f init_node/compose.yml -p chord up 
else
  echo "Clusτer was already created. Restarting Nodes..."
  docker compose -f init_node/compose.yml -p chord start 
fi

while true
do 
echo "Init node is alive: $(date +"%T")"
sleep 100
done

# docker compose -p chord down --> to bring down havoc. Alternatively, use stop 

