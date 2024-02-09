from __netsetup__ import (
    setup_network,
    project_config
)

from ChordSeek import (
    lookup,
    leave,
    join,
    _dnet_inspect,
    click
)
import grpc
from string import ascii_lowercase
from random import choices, randint
from importlib import import_module
from math import log2
from time import sleep




def benchmark_lookup(network, ChordStub, node_replicas, Empty, reps):
    
    
    hops_count = 0
    hops_per_node = list()
    for _ in range(reps):
        random_text = "".join(choices(ascii_lowercase, k = 5))
        random_uint = randint(0, 6)
              
        lookup.callback(random_text,random_uint) 

    for _, node_ip in network:
        with grpc.insecure_channel(f"{node_ip}"+":50051") as channel:
            client = ChordStub(channel)
            hops = client.clear_hops(Empty.Empty()).num_hops
            hops_per_node.append(hops)
            hops_count += hops
        
    hops_per_node = list(map(lambda hp: (1/reps)*hp, hops_per_node))
    
    

    print(f"Total hops on average: {(1/reps)*hops_count} | expecting O(log{node_replicas}) = {log2(node_replicas)}")    
    print(f"Average hops per node per lookup: {hops_per_node}")
        

def benchmark_leave(ChordStub, node_replicas, Empty, reps):

    hops = list()
    
    for _ in range(reps): 
        join.callback()
        sleep(1.0)
        network = _dnet_inspect()
        for _, node_ip in network:
            with grpc.insecure_channel(f"{node_ip}"+":50051") as channel:
                client = ChordStub(channel)
                client.clear_hops(Empty.Empty())
              
              
    for _ in range(reps):             
        network = _dnet_inspect()
        leave.callback(network[-1][1])
        network = _dnet_inspect()
        for _, node_ip in network:
            with grpc.insecure_channel(f"{node_ip}"+":50051") as channel:
                client = ChordStub(channel)
                hops.append(client.clear_hops(Empty.Empty()).num_hops)
        
    
    print(f"Total average hops: {(1/reps)*sum(hops)} | expecting O(log{node_replicas}^2) = {log2(node_replicas)**2}")
    
    
    

def benchmark_join(ChordStub, node_replicas, Empty, reps):
    
    hops = list()
    
    for _ in range(reps): 
        join.callback()
        network = _dnet_inspect()
        for _, node_ip in network:
            with grpc.insecure_channel(f"{node_ip}"+":50051") as channel:
                client = ChordStub(channel)
                hops.append(client.clear_hops(Empty.Empty()).num_hops)
              
              
    for _ in range(reps):             
        network = _dnet_inspect()
        leave.callback(network[-1][1])
        network = _dnet_inspect()
        for _, node_ip in network:
            with grpc.insecure_channel(f"{node_ip}"+":50051") as channel:
                client = ChordStub(channel)
                client.clear_hops(Empty.Empty())
        
    
    print(f"Total average hops: {(1/reps)*sum(hops)} | expecting O(log{node_replicas}^2) = {log2(node_replicas)**2}")
    pass


@click.command()
@click.argument("option", type = str, default = "lookup")
@click.argument("reps", type = int, default = 100)
def benchmark_wrapper(option, reps):
    try:
        setup_network()
        chordprot_pb2 = import_module(".chordprot_pb2", package = "protobufs.generated")
        chordprot_pb2_grpc = import_module(".chordprot_pb2_grpc", package = "protobufs.generated")
        network = _dnet_inspect()
        ChordStub = getattr(chordprot_pb2_grpc, 'ChordStub')
        Empty = getattr(chordprot_pb2,"google_dot_protobuf_dot_empty__pb2")
        
        
        
        #point to potentially gather hops during initialization of the chord network
        for _, node_ip in network:
            with grpc.insecure_channel(f"{node_ip}"+":50051") as channel:
                client = ChordStub(channel)
                client.clear_hops(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty())

        print(f"Hop counters have been globally reset!")
        
        if option == "lookup":
            benchmark_lookup(network,ChordStub,project_config["compose"]\
                                                            ["variables"]\
                                                            ["NODE_REPLICAS"],
                                                            Empty, reps)
            
        elif option == "leave":
            benchmark_leave(ChordStub, project_config["compose"]\
                                                            ["variables"]\
                                                            ["NODE_REPLICAS"], 
                                                            Empty, reps)
        elif option == "join":
            benchmark_join(ChordStub, project_config["compose"]\
                                                            ["variables"]\
                                                            ["NODE_REPLICAS"], 
                                                            Empty, reps) 
        else:
                print(f"Invalid choice. Aborting...")
    except Exception as e:
        print(f"Error occured: {e}")
        exit(1)


if __name__ == '__main__':
    benchmark_wrapper()