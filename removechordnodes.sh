#!/bin/bash

for cont in $(docker container ls -a -f name='^test*'); do
    docker container rm $cont 2> /dev/null
done