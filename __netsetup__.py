import docker
import yaml
import os
import sys
from grpc_tools import protoc
from python_on_whales import DockerClient
from subprocess import call
from rich.console import Console
from time import sleep

def setup_docker_net(client, chordname: str, networkOptions: dict): 
    '''
    setup_docker_net
    ================
    
    Sets up a network(into which containers modeling the behavior of the Chord nodes will be introduced afterward) with the specified options.

    This method checks if a network with the given name already exists. It returns the existing network, if the network already exists.
    Otherwise, it creates and returns a new network with the provided options.

    Args:
        client (docker.DockerClient): The Docker client instance.
        chordname (str): The name of the network.
        networkOptions (dict): A dictionary containing network options including 'subnet', 'gateway', and 'ip_range'.
    
    Raises:
        docker.errors.APIError: If there is an error during the Docker API call.

    Returns:
        docker.models.networks.Network or None: The network object if it exists or is created successfully, None otherwise.
    
    '''

    try: 
        docker_network = client.networks.list(names = chordname)

        
        if len(docker_network) > 0:
            # print(f"Network: {chordname} already exists. Proceeding...")
            return docker_network[0]
    
        # print(f"Network: {chordname} does not exist. Creating...")
        ipam_pool = docker.types.IPAMPool(
            subnet = networkOptions['subnet'],
            gateway =  networkOptions['gateway'],
            iprange = networkOptions['ip_range']
        )
        ipam_config = docker.types.IPAMConfig(
            pool_configs = [ipam_pool]
        )
        return client.networks.create(chordname, driver = "bridge", ipam = ipam_config)
    
    except docker.errors.APIError as e:
        print(f"Error occured while creating the network: {e}")
        
        
    
def setup_docker_volumes(client,volumes,paths):
    '''
    setup_docker_volumes
    ====================
    
    Sets up Docker volumes with specified names and paths.

    This method checks if each specified volume already exists. If a volume exists, it proceeds. 
    If a volume does not exist, it creates the volume(using the local driver) with the specified options(including the volume name and the corresponding path).

    Args:
        client (docker.DockerClient): The Docker client instance.
        volumes (List[str]): A list of volume names to be created or checked.
        paths (List[str]): A list of paths to bind to each corresponding volume.
    
    Raises:
        docker.errors.APIError: If there is an error during the Docker API call.

    Returns:
        None
            
    '''

    try: 
        docker_volumes = client.volumes.list()


        for volume,path in zip(volumes,paths):
            if volume in map(lambda vol: vol.name,docker_volumes):
                 # print(f"Volume: {volume} already exists. Proceeding...")
                 pass
            else:
                # print(f"Volume: {volume} wasn't found. Creating...")
                client.volumes.create(name = volume, 
                                      driver = "local",
                                      driver_opts = {"type":"none", 
                                                     "o":"bind",
                                                     "device": os.path.join(os.environ["PWD"],path)}) 


    except docker.errors.APIError as e:
        print(f"Error occured while creating the volumes: {e}")

with open(os.path.join("./","project_config.yml"),'r') as config:
    project_config = yaml.load(config, Loader = yaml.FullLoader)

def setup_network()-> None:
    '''
    setup_network
    =============
    
    Sets up the network, the appropriate volumes and the Docker Compose environment for the Chord project.

    Reads configuration from 'project_config.yml' to determine network options, volumes, protobuffer paths and Docker Compose settings. 
    Creates or verifies the network, sets up required volumes, compiles Protobuf files and starts the Docker Compose environment.

    Raises:
        KeyError: If a required env variable is missing in the project configuration.
        OSError: If there is an error while listing the directory with database files.
        Exception: For other unexpected errors.
    
    Returns:
        None
    
    '''
    console = Console()
    if os.path.exists(project_config['environment_file']):
        with open(project_config['environment_file'], "r") as environment_file:
            data = yaml.safe_load(environment_file)
            if data['CHORD_INIT'] == 'true':
                with console.status("[bold orange3]"f"Chord network is already configured. Proceeding..."):
                  sleep(1)
                return
    
    client = docker.from_env()
    
    chord_network = setup_docker_net(client, project_config['network_name'], project_config['network_options'])
    
    setup_docker_volumes(client, project_config['volumes']['names'], project_config['volumes']['paths'])

    # print(f"Compiling protobuffer. Proceeding...")


    try:
        
        generated_stubs_path = os.path.join(os.getcwd(), project_config['protobuffer']['path'], "generated")
        if not os.path.exists(generated_stubs_path):
            # print(f"Generating stubs in {generated_stubs_path}")
            os.makedirs(generated_stubs_path) 
        

            protoc.main(
                (
                    "",
                    f"--proto_path={project_config['protobuffer']['path']}",
                    f"--proto_path=dhtChordVenv/lib/python3.8/site-packages/grpc_tools/_proto/", 
                    f"--python_out={generated_stubs_path}", 
                    f"--grpc_python_out={generated_stubs_path}",
                    f"{os.path.join(project_config['protobuffer']['path'],'chordprot.proto')}",
                )
            )
            
            return_code_touch = call(f"touch {os.path.join('.',project_config['volumes']['paths'][3], '__init__.py')}", shell = True)
            return_code_sed = call(f"sed -i 's/import chordprot_pb2 as chordprot__pb2/from . import chordprot_pb2 as chordprot__pb2/' \
                               {os.path.join('.',project_config['volumes']['paths'][3],'chordprot_pb2_grpc.py')}", shell = True)
            
            # chordprot_pb2 = import_module(".chordprot_pb2", package = "protobufs.generated")
            # chordprot_pb2_grpc = import_module(".chordprot_pb2_grpc", package = "protobufs.generated")
            
        
        else:
            pass
            # print(f"Protobuffer is already compiled and stubs are present in {generated_stubs_path}")
        # print(chord_network.containers)

        env_variables_size = len(project_config['compose']['variables'])
        for index, (key,val) in enumerate(project_config['compose']['variables'].items()):
            if index < env_variables_size - 1:
                os.environ[key] = str(val)
            else:
                directory_path = os.path.join(project_config['volumes']['paths'][0])
                if not os.path.exists(directory_path):
                    os.makedirs(directory_path)
                    os.environ[key] = "miss"
                else:
                    db_files = [db for db in os.listdir(os.path.join(directory_path)) if db.endswith(".db")]
                    #print(len(db_files) > 0)
                    os.environ[key] = "hit" if len(db_files) > 0 else "miss" 
        
        os.environ["NNAME"] = project_config['network_name']
        # for key,val in project_config['compose']['variables'].items():
        #           print(f"{os.environ[key]}")
                         
                

        whales_docker_client = DockerClient(compose_files = [project_config['compose']['path']], 
                                            compose_project_name = project_config['compose']['name'])

        whales_docker_client.compose.up(recreate = True, detach = True, quiet = True)
        with console.status("[bold yellow]"f"Previous configuration for chord network was not found. Shooting up..."):
            sleep(2)
            while init_info:=client.containers.get("init_node").attrs['State']['Status'] == 'running':
                # print(client.containers.get("init_node").logs(follow = True, stream = True))
                # for el in client.containers.get("init_node").logs(follow = True, stream = True):
                #     print(el)
                pass
            else:
                pass
        with console.status("[bold green3]"f"Initilization of the chord network has been completed successfully. Proceeding..."):
            sleep(1)
                
                
                
        
        with open(project_config['environment_file'],"w") as modified_project_config:
             yaml.dump({"CHORD_INIT":"true"}, modified_project_config)
            
    except KeyError as e:
        print(f"Failed setting environement variables properly. Error: {e}")
    except os.error as e:
        print(f"Failed listing directory with database files. Error: {e}")
    except ImportError as e:
        print(f"Failed to import generated Stubs. Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

    




if __name__ == '__main__':
    setup_network()
    
    