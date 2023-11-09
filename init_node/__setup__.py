import grpc 
#from subprocess import run
import os
import docker
from ChordNodeCode.chordprot_pb2_grpc import ChordStub
from ChordNodeCode.chordprot_pb2 import (
    JoinRequest
)
from random import shuffle
from time import sleep

class ChordInitialization:
    
    def __init__(self):
        #TODO: add a config.yaml to get the environment variables for the settings.
        self.netname = os.environ.get("NET_NAME","chord-net")
        self.container_id = os.environ.get("CONTAINER_ID","")
        self.nodes = int(os.environ.get("NODE_REPLICAS",32))

        self.network = self._dnet_inspect()
        self.active_chord = list()
        self.hops = 0

    def _dnet_inspect(self):
        client = docker.from_env()
        network = list()
        try:
            net_info = client.api.inspect_network(self.netname)
            containers = net_info["Containers"]

            for key,value in containers.items():
                if key != self.container_id:
                    network.append((value["Name"],value["IPv4Address"]))

        except docker.errors.APIError as e:
            print(f"The {network_name} docker network wasn't found. Critical failure...")
            #return a sorted (ip address) list of nodes
        return sorted(network, key = lambda x: int(x[1].split(".")[3].split("/")[0]))

    def initialize(self):
        #TODO: check altering the channel each time.
        print(f"Initial hosts in network: {len(self.network)}")

        shuffle(self.network)
        elected_host = self.network.pop()
        print(f"Election begins...")
        sleep(1.5)
        print(f"Elected Node for initilization: {elected_host}")

        channel = grpc.insecure_channel(elected_host[0]+":50051")
        
        client = ChordStub(channel)
        self.hops = client.join(JoinRequest(elected_host[1]), init=True).num_hops
        self.active_chord.append(elected_host)

        for i in range(self.nodes -1):
            shuffle(self.network)
            arbitary_node = self.active_chord[random.randint(0,len(self.active_chord)-1)]
            elected_host = self.network.pop()
            print(f"[{i}]: Election begins...")
            print(f"[{i}]: Elected Node for initilization: {elected_host}")
            channel = grpc.insecure_channel(elected_host[0]+":50051")
            client = ChordStub(channel)
            self.hops = self.hops + client.join(JoinRequest(arbitary_node)).num_hops
            self.active_chord.append(elected_host)


            
       

        

chinit = ChordInitialization()
# print(type(chinit.network))
# for el in chinit.network:
#     print(el)

#chinit.initialize()

