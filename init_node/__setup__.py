import grpc 
#from subprocess import run
import os
import yaml
import docker

#python path is updated through netsetup.sh script so that the following
#expression is interpreted successfully.
from ChordNodeCode.chordprot_pb2_grpc import ChordStub
from ChordNodeCode.chordprot_pb2 import (
    JoinRequest
)
from random import (
    shuffle,
    randint
)
from time import sleep

with open(os.path.join('./init_node','config.yml'), 'r') as config:
    config_file = yaml.load(config, Loader = yaml.FullLoader)

class ChordInitialization:  
    '''
    Behavior class for initializing a chord network.
    '''

    def __init__(self):
        self.netname = os.environ.get(config_file['network_var'])
        self.container_id = os.environ.get(config_file['container_var'])
        self.nodes = int(os.environ.get(config_file['replicas_var']))

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
            print(f"The {self.netname} docker network wasn't found. Critical failure...")
            #return a sorted (ip address) list of nodes
        return sorted(network, key = lambda x: int(x[1].split(".")[3].split("/")[0]))

    def initialize(self):
        #TODO: check altering the channel each time.
        print(f"Initial hosts in network: {len(self.network)}")

        shuffle(self.network)
        elected_host = self.network.pop()
        print(f"Election begins...")
        sleep(1.5)
        print(f"Initial elected Node: {elected_host}")
        channel = grpc.insecure_channel(elected_host[0]+":50051")
        
        client = ChordStub(channel)
        self.hops = client.join(JoinRequest(ip_addr = elected_host[1], init = True)).num_hops
        self.active_chord.append(elected_host)

        for i in range(self.nodes -1):
            shuffle(self.network)
            arbitary_node = self.active_chord[randint(0,len(self.active_chord)-1)]
            elected_host = self.network.pop()
            print(f"[{i}]: Election begins...")
            sleep(0.3)
            print(f"[{i}]: Elected Node for initilization: {elected_host}")
            channel = grpc.insecure_channel(elected_host[0]+":50051")
            client = ChordStub(channel)
            self.hops = self.hops + client.join(JoinRequest(ip_addr=arbitary_node[1])).num_hops
            self.active_chord.append(elected_host)


            
       

        
if __name__ == "__main__":
    chinit = ChordInitialization()
    chinit.initialize()
    print(f"Total hops: {chinit.hops}\nNetwork size: {len(chinit.active_chord)}")
    for el in chinit.active_chord:
        print(el)

#print(chinit.netname, chinit.container_id,chinit.network,chinit.nodes)
# print(type(chinit.network))
# for el in chinit.network:
#     print(el)



