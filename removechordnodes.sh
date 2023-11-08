#!/bin/bash

for cont in $(docker container ls -a -f name='^init_node_chord*'); do
    docker container rm $cont 2> /dev/null
done
