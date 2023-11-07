FROM ubuntu:latest

RUN apt update -y
RUN apt install python3 python3-pip curl docker-compose -y
RUN python3 -m pip install grpcio grpcio-tools

RUN mkdir -p ~/.docker/cli-plugins
RUN curl -SL https://gist.githubusercontent.com/thaJeztah/b7950186212a49e91a806689e66b317d/raw/36d4c5854523502deed5b56842b0d34cd6ec0f70/docker-compose-plugin.sh > ~/.docker/cli-plugins/docker-compose

RUN chmod +x  ~/.docker/cli-plugins/docker-compose

WORKDIR /opt/chord


VOLUME ["/var/run/docker.sock","/usr/bin/docker","./init_node"]

ENTRYPOINT [ "/bin/bash" ]
