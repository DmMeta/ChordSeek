FROM ubuntu:latest

RUN apt update -y
RUN apt install --no-install-recommends --yes python3 python3-pip iputils-ping iproute2
RUN python3 -m pip install grpcio grpcio-tools
RUN python3 -m pip install -U "watchdog[watchmedo]"

RUN export PATH="${PATH:+${PATH}:}/usr/local/lib/python3.10/dist-packages/"