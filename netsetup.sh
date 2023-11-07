NET_NAME="chord-net"
NET_OPTIONS="--driver=bridge --subnet=10.0.0.0/25 --gateway=10.0.0.1 --ip-range=10.0.0.2/26"

CONTAINER_ID=$(basename $(cat /proc/1/cpuset))  


EXISTING_NET=$(docker network ls | grep $NET_NAME)


if [ -z "$EXISTING_NET" ]; then
    echo "The network with name: '$NET_NAME' does not exist.Creating..."
    docker network create $NET_OPTIONS $NET_NAME 
    docker network connect --ip=10.0.0.2 $NET_NAME $CONTAINER_ID
else
     echo "The network with name: '$NET_NAME' already exists.Proceeding..."
     if [[ $(docker network inspect "$NET_NAME") == *"$CONTAINER_ID"* ]]; then
        docker network connect --ip=10.0.0.2 $NET_NAME $CONTAINER_ID
     fi
     
fi




