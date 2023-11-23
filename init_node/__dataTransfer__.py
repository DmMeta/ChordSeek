import grpc
import logging
from generatedStubs.chordprot_pb2_grpc import ChordStub, DataTransferStub
from generatedStubs.chordprot_pb2 import (
    SuccessorRequest,
    CompScientistData,
    DataTransferRequest
)

from random import randint



class DataTransfer:
    
    def __init__(self, data, network):
     
     self.logger = logging.getLogger(__name__)
     logging.basicConfig(level=logging.WARNING)
    #  self.netname = os.environ.get(config_file['chord']['network_var'])
    #  self.container_id = os.environ.get(config_file['chord']['container_var'])
     self.network = network
     self.scientists = data
    #  try:
    #     with open(os.path.join("./init_node/scientists.json"), "r", encoding="utf-8") as scientist_file:
    #         self.scientists = json.load(scientist_file)
        
    #  except FileNotFoundError as e:
    #      self.logger.error(f"Scientists.json file not found in {os.path.join('./init_node')}.\n Stack Trace: {e}")
    #  except json.JSONDecodeError as e:
    #      self.logger.error(f"Scientists.json file is not a valid JSON file. Error decoding the file.\n Stack Trace: {e}")
    #  except Exception as e:
    #      self.logger.error(f"Unexpected file error occured!\nStack Trace: {e}")
    #      self.scientists = {}

    def transmitData(self, hash_fun):
        print(len(self.scientists.keys())) 


        for key, value in self.scientists.items():
            hash_value = hash_fun(key)
            elected_node = self.network[randint(0,len(self.network)-1)][1]
            # print(f"Hash value of key: {hash_value}")
           
            try:  
                with grpc.insecure_channel(f"{elected_node}:50051") as channel:
                    client = ChordStub(channel)
                    node = client.find_successor(SuccessorRequest(key_id = hash_value))
                    # print(f"Sending to {node}")
                with grpc.insecure_channel(f"{node.ip_addr}:50051") as channel:
                     client = DataTransferStub(channel)
                     dt = DataTransferRequest(data = map(lambda sc:  CompScientistData(Surname = sc['Surname'], 
                                                                              Education = sc['Education'], 
                                                                              Awards = sc['Awards'],
                                                                              Hash = hash_value), #adding hash_value
                                            value))                     
                     client.store(dt)
                
                    
            except Exception as e:
                self.logger.error(f"Error during transmission occured: {e}")
    
        
             

if __name__ == "__main__":
    dt = DataTransfer()
    dt.transmitData()


