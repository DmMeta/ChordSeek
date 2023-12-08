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
from concurrent import futures
import requests
from bs4 import BeautifulSoup
from time import sleep

# def lookup_wrapper(args):
#     t1, t2, random_node,ChordStub,Empty = args
    
#     with grpc.insecure_channel(f"{random_node[1]}:50051") as channel:
#         client = ChordStub(channel)
#         hops_response = client.clear_hops(Empty.Empty())
#         return hops_response.num_hops
    

# def benchmark_lookup(network,ChordStub,id_space_exp,Empty):

#     hops = list()

#     def generate_random_input():
#       random_node = network[randint(0, len(network)-1)]
#       random_text = "".join(choices(ascii_lowercase, k = 5))
#       random_uint = randint(0, 12)
#       return random_text, random_uint, random_node,ChordStub,Empty

    
#     passed_args = [generate_random_input() for _ in range(1000)]

#     with futures.ProcessPoolExecutor(max_workers = 4) as executor:
#         results = list(executor.map(lookup_wrapper, passed_args))
    
#     hops.extend(results)

#     bad_cases = list(filter(lambda hops: hops > log2(id_space_exp), hops))
#     average_cases_cnt = len(hops) - len(bad_cases)
#     if len(bad_cases) != 0:
#         print(f"Out of 1000000 lookups we got {len(bad_cases)} averaging at {(1/len(bad_cases))*sum(bad_cases)} \
#     and {average_cases_cnt} average cases.")
#     else: 
#         print(f"We got {average_cases_cnt} average_cases (O(logn) lookup time) and {len(bad_cases)}")
            

# def get_random_universities(reps):


#     url = f"https://www.randomlists.com/random-colleges?dup=false&qty={reps}"

#     # Make a request to the page
#     response = requests.get(url)

#     # Check if the request was successful (status code 200)
#     if response.status_code == 200:
#         # Parse the HTML content of the page
#         soup = BeautifulSoup(response.text, 'html.parser')

#         university_list = soup.find_all('div', class_ = "Rand-stage")
#         print(type(university_list))
#         for el in university_list:
#             print(f"{el.text}")
#         # if university_list:
#         #     # Extract the list of universities
#         #     universities = university_list.find('li')

#         #     # Print the random universities
#         #     for index, university in enumerate(universities, start=1):
#         #         print(f"{index}. {university.get_text().strip()}")

#     else:
#         print(f"Failed to retrieve data. Status code: {response.status_code}")



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