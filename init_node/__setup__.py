import grpc 
#from subprocess import run
import os
import docker
import yaml

#python path is updated through netsetup.sh script so that the following
#expression is interpreted successfully.
from generatedStubs.chordprot_pb2_grpc import ChordStub 
from generatedStubs.chordprot_pb2 import (
    JoinRequest
)
from random import (
    shuffle,
    randint
)
from time import sleep
from generatedStubs.chordprot_pb2_grpc import google_dot_protobuf_dot_empty__pb2 as google_pb_empty


class ChordInitialization:  
    '''
    Behavior class for initializing a chord network.
    '''

    def __init__(self, config_file):
        #These probably should change. Applying the change.
        self.netname = os.environ.get(config_file['chord']['network_var'])
        try:
            client = docker.from_env()
            print(f"client dockinit| {os.environ.get(config_file['chord']['container_var'])}")
            
            self.container_id = client.containers.get(os.environ.get(config_file['chord']['container_var'])).id
        except docker.errors.APIError as e:
             print(f"Failed to get container id for init_node")
        # self.nodes = int(os.environ.get(config_file['chord']['replicas_var']))

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
                    network.append((value["Name"],value["IPv4Address"].split("/")[0]))

        except docker.errors.APIError as e:
            print(f"The {self.netname} docker network wasn't found. Critical failure...")
            #return a sorted (ip address) list of nodes
        return sorted(network, key = lambda x: int(x[1].split(".")[3]))

    def initialize(self):
        
        print(f"Initial hosts in network: {len(self.network)}")
        network_size = len(self.network)

        #shuffle(self.network)
        elected_host = self.network.pop()
        print(f"Election begins...")
        sleep(1.5)
        print(f"Initial elected Node: {elected_host}")
        channel = grpc.insecure_channel(elected_host[0]+":50051")
        
        client = ChordStub(channel)
        self.hops = client.join(JoinRequest(ip_addr = elected_host[1], init = True)).num_hops
        self.active_chord.append(elected_host)

        for i in range(network_size - 7): 
            #shuffle(self.network)
            arbitary_node = self.active_chord[randint(0, len(self.active_chord) - 1)] 
            print(arbitary_node)
            elected_host = self.network.pop()
            print(f"[{i}]: Election begins...")
            # sleep(0.3)
            print(f"[{i}]: Elected Node for initilization: {elected_host}")
            channel = grpc.insecure_channel(elected_host[1]+":50051")
            client = ChordStub(channel)
            self.hops = self.hops + client.join(JoinRequest(ip_addr = arbitary_node[1])).num_hops
            self.active_chord.append(elected_host)


            
       

        
if __name__ == "__main__":
    with open(os.path.join('./init_node','config.yml'), 'r') as config:
                config_file = yaml.load(config, Loader = yaml.FullLoader)
    chinit = ChordInitialization(config_file = config_file)
    chinit.initialize()
    print(f"Total hops: {chinit.hops}\nNetwork size: {len(chinit.active_chord)}")
    for el in chinit.active_chord:
        print(el)
    

#print(chinit.netname, chinit.container_id,chinit.network,chinit.nodes)
# print(type(chinit.network))
# for el in chinit.network:
#     print(el)



