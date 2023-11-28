import click
import random
from __netsetup__ import (
    setup_network,
    project_config    
)
from rich.console import Console
from rich.table import Table, box
from rich.style import Style
from rich.text import Text
from time import sleep
from importlib import import_module
import hashlib
import grpc
import docker
from random import randint
from google.protobuf.json_format import MessageToDict


@click.group()
def cli():
    "Implementation of Chord protocol for wikipedia's famous computer scientists dataset."
    try:
        setup_network()
    except Exception as e:
        print(f"Error during network setup: {e}")
        exit(1)
    



@cli.command()
@click.option('--university', type=str, metavar='UNIVERSITY', help ='Name of the university to search for computer scientists.')
@click.option('--awards', type=int, metavar='AWARDS', help = 'Minimum number of awards per computer scientist.')
def lookup(university: str, awards: int):
    """
    Distributed lookup for computer scientists
    from a specific university with a minimum number of awards.
    
    """

    try:
        if awards < 0:
            raise ValueError
        console = Console() 
        key_value = hash(university)
        network = _dnet_inspect() #TODO: maybe we want active_chord, not network. 
        arbitary_node = network[randint(0, len(network) - 1)]
        chordprot_pb2 = import_module(".chordprot_pb2", package = "protobufs.generated")
        chordprot_pb2_grpc = import_module(".chordprot_pb2_grpc", package = "protobufs.generated")
        
        ChordStub = getattr(chordprot_pb2_grpc, "ChordStub")
        SuccessorRequest = getattr(chordprot_pb2, "SuccessorRequest")
        DataTransferStub = getattr(chordprot_pb2_grpc, "DataTransferStub")
        RangeQueryRequest = getattr(chordprot_pb2, "RangeQueryRequest")
        
        with grpc.insecure_channel(arbitary_node[1]+":50051") as channel:
            client = ChordStub(channel)
            corresponding_node = client.find_successor(SuccessorRequest(key_id = key_value))
            
        with grpc.insecure_channel(corresponding_node.ip_addr+":50051") as channel:
            client = DataTransferStub(channel)
            data = client.get_data(RangeQueryRequest(university = university, max_awards = awards))       
            dict_data = MessageToDict(data, including_default_value_fields = True)  
            
        with console.status("[bold light_steel_blue1]"f"Searching at {university} for computer scientists with at least {awards} awards. [bold green]Processing..."):
            sleep(0.9)
            if len(dict_data['data']) > 0:
                table = Table(title=f"\nComputer Scientists Fetched", box = box.ROUNDED, show_lines = True)
                table.add_column("Surname", justify = "left", style = "navajo_white3", no_wrap = True)
                table.add_column("Education", justify = "left", style = "light_steel_blue1", no_wrap = True)
                table.add_column("Awards", justify = "left", style = "sandy_brown")
                for record in dict_data['data']:
                    table.add_row(record["Surname"], record["Education"], str(record["Awards"]))

                console.print(table)
            else:
                warning_console = Console(stderr=True, style="orange3")
                warning_console.print(f"\n[bold]No data records found that meets the criteria(awards >= {awards}) you specified for the provided university({university}).[/bold]")
            
    except ValueError as e:
        error_console = Console(stderr=True, style="red")
        error_console.print("[bold red] <Awards> [/bold red]" "should be a positive integer.")
    except grpc.RpcError as e:
        error_console = Console(stderr = True, style = "red")
        error_console.print("[bold red] <Fatal Error> [/bold red]" "during transimission.")
        print(f"Error: {e}")
    except Exception as e:
        error_console.print(f"An unexpected error occurred.")


@cli.command()
@click.option('--node_ip', type=str, metavar='NODE_IP_ADDRESS', help ='The IP address of the node you want to view the finger table.')
def findFT(node_ip: str):
    """
    Takes an IP address and locates the corresponding finger table for that node.
    
    """
    console = Console()
    node = hash(node_ip)
    
    try:
        
        with grpc.insecure_channel(node_ip+":50051") as channel:
            chordprot_pb2_grpc = import_module(".chordprot_pb2_grpc", package = "protobufs.generated")
            DataTransferStub = getattr(chordprot_pb2_grpc, "DataTransferStub")
            client = DataTransferStub(channel)
            data = client.get_finger_table(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 
            dict_data = MessageToDict(data, including_default_value_fields = True)  

        with console.status("[bold light_steel_blue1]"f"Searching for the finger table of node {node}. [bold green]Processing..."):
            sleep(0.9)
            table = Table(title=f"\nFinger Table of node {node}", box = box.ROUNDED, show_lines = True)
            table.add_column("start", justify = "left", style = "navajo_white3", no_wrap = True)
            table.add_column("int.", justify = "left", style = "pale_turquoise4", no_wrap = True)
            table.add_column("successor", justify = "left", style = "light_steel_blue1", no_wrap = True)
            table.add_column("successor's ip", justify = "left", style = "sandy_brown")
            for index, record in enumerate(dict_data['data']):
                if index + 1 < len(dict_data['data']):
                    chord_rng = f"[{record['start']}, {dict_data['data'][index+1]['start']})"
                else:
                    chord_rng = f"[{record['start']}, {node})"
                table.add_row(str(record["start"]), chord_rng, str(record["node"]), str(record["nodeIp"]))
            console.print(table)
            
    except grpc.RpcError as e:
        error_console = Console(stderr = True, style = "red")
        error_console.print(f"Fatal Error during transimission.")
        print(e)
    except Exception:
        error_console.print(f"An unexpected error occurred.")


@cli.command()
@click.option('--node_ip', type=str, metavar='NODE_IP_ADDRESS', help ='The IP address of the node you want to find the successor.')
def FindSuccessor(node_ip: str):
    """
    Takes an IP address and locates the corresponding successor for that node.
    
    """
    console = Console()
    node = hash(node_ip)
    
    try:
        with grpc.insecure_channel(node_ip+":50051") as channel:
            chordprot_pb2_grpc = import_module(".chordprot_pb2_grpc", package = "protobufs.generated")
            ChordStub = getattr(chordprot_pb2_grpc, "ChordStub")
            client = ChordStub(channel)
            data = client.get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 
            
    
        with console.status("[bold light_steel_blue1]"f"Searching for the successor of node {node}. [bold green]Processing..."):
            sleep(2)
            table = Table()
            table.add_column("Current Node", style="navajo_white3", justify="center", no_wrap=True)  
            table.add_column("Current Node's IP address", style="navajo_white3", justify="center", no_wrap=True) 
            table.add_column("Successor Node", style="navajo_white3", justify="center", no_wrap=True)
            table.add_column("Successor Node's IP address", style="navajo_white3", justify="center", no_wrap=True) 
            table.add_row(str(node), str(node_ip), str(data.node_id), str(data.ip_addr))
            console.print(table)
    
    except grpc.RpcError as e:
        error_console = Console(stderr = True, style = "red")
        error_console.print(f"Fatal Error during transimission.")
        print(e)
    except Exception:
        error_console.print(f"An unexpected error occurred.")



@cli.command()
@click.option('--node_ip', type=str, metavar='NODE_IP_ADDRESS', help ='The IP address of the node you want to find the predecessor.')
def FindPredecessor(node_ip: str):
    """
    Takes an IP address and locates the corresponding predecessor for that node.
    
    """
    console = Console()
    node = hash(node_ip)
    
    try:
        with grpc.insecure_channel(node_ip+":50051") as channel:
            chordprot_pb2_grpc = import_module(".chordprot_pb2_grpc", package = "protobufs.generated")
            ChordStub = getattr(chordprot_pb2_grpc, "ChordStub")
            client = ChordStub(channel)
            data = client.get_predecessor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 
            
    
        with console.status("[bold light_steel_blue1]"f"Searching for the predecessor of node {node}. [bold green]Processing..."):
            sleep(2)
            table = Table()
            table.add_column("Current Node", style="navajo_white3", justify="center", no_wrap=True)  
            table.add_column("Current Node's IP address", style="navajo_white3", justify="center", no_wrap=True) 
            table.add_column("Predecessor Node", style="navajo_white3", justify="center", no_wrap=True)
            table.add_column("Predecessor Node's IP address", style="navajo_white3", justify="center", no_wrap=True) 
            table.add_row(str(node), str(node_ip), str(data.node_id), str(data.ip_addr))
            console.print(table)
    
    except grpc.RpcError:
        error_console = Console(stderr = True, style = "red")
        error_console.print(f"Fatal Error during transimission:""[bold red] <Invalid IP address!>[/bold red]")
    except Exception:
        error_console.print(f"An unexpected error occurred.")
    

@cli.command()
def join():
    """
    Joins a node to the Chord network.
    
    """
    client = docker.from_env()
    console = Console()
 
    try:
        network = _dnet_inspect()
        container = client.containers.run(
            'chord_node:v1.0',           
            detach = True,
            network = project_config['network_name'],
            name = f"chord-chordNode-{len(network)+1}",
            mem_limit = '100m',
            cpu_period = 100000,
            cpu_quota = 50000,
            volumes = {
                f"{project_config['volumes']['names'][0]}": {'bind': '/opt/chordNode/Data/'},
                f"{project_config['volumes']['names'][1]}": {'bind': '/opt/chordNode/'},
                f"{project_config['volumes']['names'][3]}": {'bind': '/opt/chordNode/generatedStubs/'}   
            },
            entrypoint =  ["/usr/bin/watchexec","-f","chordNode.py","-c","-r", "--","python3 ./chordNode.py"],
            environment = {
                "FT_SIZE" : f"{project_config['compose']['variables']['IDENT_SPACE_EXP']}"
            }
        )
       
        chord_net_containers = client.networks.get(project_config["network_name"]).containers
        jcontainer = filter(lambda cont: cont.name == f"chord-chordNode-{len(network)+1}", chord_net_containers)
        if len(jnode:=list(jcontainer)) > 0:
            jnode_ip_address = jnode[0].attrs["NetworkSettings"]["Networks"][project_config['network_name']]["IPAddress"]
        else:
            raise docker.errors.APIError("New network'ip wasn't found")
        with console.status("[bold light_steel_blue1]"f"Container was successfully created and joined to network '{project_config['network_name']}'. [bold green]Processing..."):
            sleep(2)
          
             
            arbitary_node = network[randint(0, len(network) - 1)]
            with grpc.insecure_channel(str(jnode_ip_address)+":50051") as channel:
                chordprot_pb2 = import_module(".chordprot_pb2", package = "protobufs.generated")
                chordprot_pb2_grpc = import_module(".chordprot_pb2_grpc", package = "protobufs.generated")
 
                ChordStub = getattr(chordprot_pb2_grpc, "ChordStub")
                JoinRequest = getattr(chordprot_pb2, "JoinRequest")
                client = ChordStub(channel)
                client.join(JoinRequest(ip_addr = arbitary_node[1] , transfer_data = True))
                console.print(f"[bold green]Successful join of node chord-chordNode-{len(network)+1} at chord network...")
    except KeyError as e:
        print(f"key error: {e}")
    except grpc.RpcError as e:
        error_console = Console(stderr = True, style = "red")
        error_console.print(f"Error during join gRPC call!")
        print(e)        
    except docker.errors.APIError as e:
        print(f"Error creating/joining container: {e}")        


@cli.command()
@click.option('--node_ip', type=str, metavar='NODE_IP_ADDRESS', help ='[Optional] The IP address of the node you want to delete. \
If none given the leaving node is selected randomly')
def leave(node_ip = None):
    """
    Deletes a node from Chord network.
    
    """
    docker_client = docker.from_env()
    console = Console()
    chord_net = docker_client.networks.get(project_config["network_name"])
    chord_net_containers = chord_net.containers
    
    random_container = chord_net_containers[randint(0, len(chord_net_containers)-1)]
    leaving_node_ip = random_container.attrs["NetworkSettings"]["Networks"]\
                                            [project_config['network_name']]["IPAddress"]
    if node_ip is not None:
        result = list(filter(lambda cont: cont.attrs["NetworkSettings"]["Networks"]\
                                                        [project_config['network_name']]\
                                                        ["IPAddress"] == str(node_ip), 
                                 chord_net_containers))
        random_container = result[0] if len(result) > 0 else random_container
        leaving_node_ip = node_ip

    with console.status("[bold light_steel_blue1]"f"Beginning leave sequence for node with IP address: {leaving_node_ip}. [bold green]Processing..."):
        sleep(2)
        try: 
            with grpc.insecure_channel(leaving_node_ip +":50051") as channel:
                chordprot_pb2_grpc = import_module(".chordprot_pb2_grpc", package = "protobufs.generated")
                ChordStub = getattr(chordprot_pb2_grpc, "ChordStub")
                client = ChordStub(channel)
                client.leave(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty())
                
            
            chord_net.disconnect(random_container)
            random_container.stop()
            random_container.remove()
            console.print(f"[bold green]Successful leave of selected node.")
        except KeyError as e:
            print(f"key error: {e}")
        except grpc.RpcError as e:
            error_console = Console(stderr = True, style = "red")
            error_console.print(f"Error during leave gRPC call!")
            print(e)
        except docker.errors.APIError as e:
            error_console = Console(stderr = True, style = "red")
            error_console.print(f"Error during docker api call!")
            print(e)
        except Exception as e:
            error_console = Console(stderr = True, style = "red")
            error_console.print(f"Error occured: {e}")     

@cli.command()
def printNodes():
    '''
    Returns info of the network's nodes.    
    '''
    console = Console()
    docker_client = docker.from_env()
    chord_net = docker_client.networks.get(project_config["network_name"])
    chord_net_containers = sorted(chord_net.containers,
                       key = lambda cont: hash(cont.attrs["NetworkSettings"]["Networks"]\
                                                        [project_config['network_name']]\
                                                        ["IPAddress"]))
  
    table = Table()
    table = Table(title=f"\nChord Network", box = box.ROUNDED, show_lines = True)
    table.add_column("Node Name", style="navajo_white3", justify="center", no_wrap=True)  
    table.add_column("Node IP address", style="navajo_white3", justify="center", no_wrap=True) 
    table.add_column("Node's hash value", style="navajo_white3", justify="center", no_wrap=True)
    
    for container in chord_net_containers:
        node_name = container.name
        node_ip = container.attrs["NetworkSettings"]["Networks"]\
                                 [project_config['network_name']]\
                                 ["IPAddress"]
                 
        node_hashv = hash(node_ip)                                                
        table.add_row(node_name, node_ip, str(node_hashv))
    
    console.print(table)

    
    
    
    

def hash(data, modulus = project_config['compose']['variables']['IDENT_SPACE_EXP']) -> int:
        '''
        _hash_
        ======
        Computes the SHA-256 hash and returns it as an integer. 

        Args:
          data: The data to be hashed.
        
        Note:
          This method takes input data, encodes it in UTF-8, computes the SHA-256 hash
          and returns the hash value as an integer.

        Returns:
          int: The integer representation of the SHA-256 hash.

        '''
        sha256 = hashlib.sha256()
        sha256.update(data.encode('utf-8'))
        return int(sha256.hexdigest(), 16) % (2**int(modulus))    
    

def _dnet_inspect():
        client = docker.from_env()
        network = list()
        try:
            net_info = client.api.inspect_network(project_config['network_name'])
            containers = net_info["Containers"]

            for key, value in containers.items():
                    network.append((value["Name"], value["IPv4Address"].split("/")[0]))

        except docker.errors.APIError as e:
            print(f"The {project_config['network_name']} docker network wasn't found. Critical failure...")
           
        return sorted(network, key = lambda x: int(x[1].split(".")[3]))
        




# def find_succ_t(nodelist, key):
#     index = bisect_right(nodelist,key)
    
#     if index < len(nodelist):
#         return index, nodelist[index]
#     else: 
#         return 0, nodelist[0]

# assert (client.find_successor(5) == find_succ_t(nodelist,5))



if __name__ == '__main__':
    cli()
    
    
    
    
    