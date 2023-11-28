FROM ubuntu:latest

RUN apt update -y
RUN apt install --no-install-recommends --yes python3 python3-pip iputils-ping iproute2 curl
RUN python3 -m pip install grpcio grpcio-tools grpc-interceptor

RUN export PATH="${PATH:+${PATH}:}/usr/local/lib/python3.10/dist-packages/"
WORKDIR /opt/chordNode/
RUN curl -o watchexec.deb -sSL https://apt.cli.rs/pool/main/w/watchexec-cli/watchexec-1.23.0-x86_64-unknown-linux-musl.deb \ 
    && dpkg -i ./watchexec.deb && rm ./watchexec.deb; watchexec --version && echo "watchexec installation was successfull."