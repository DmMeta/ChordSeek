from concurrent.futures import ThreadPoolExecutor
import grpc

from chordprot_pb2 import (
    JoinResponse,
    JoinRequest
)

import chordprot_pb2_grpc
from dataclasses import dataclass,field
from typing import List,Tuple
from subprocess import (
    run, 
    CalledProcessError
)
import os



class ChordNode(chordprot_pb2_grpc.ChordServicer):
    '''
    Class that models a chord node. Plan is to act both as a server and as a client.
    '''

    @dataclass
    class FingerTable:
        #TODO: need to parametrize the exponent(7).
        hashed_ip_addr: int
        FT: List[Tuple[int, int, str]] = field(init=False)
        
        def __post_init__(self) -> None:
            self.__key__()

        def __key__(self) -> List[Tuple[int, int, str]]:
            ft_size = int(os.environ.get("FT_SIZE", 7))
            self.FT = [(self.hashed_ip_addr % (2**ft_size) + 2**i, 0, "") for i in range(ft_size)]
            
      

    def __init__(self) -> None:
        try: 
            result = run("hostname -I", shell = True, capture_output = True, text = True)
            self.ip_addr = result.stdout.strip()
        except CalledProcessError as e:
            print(f"Error occured: {e}")
        self.FT = self.FingerTable(hash(self.ip_addr))
        self.successor = None
        self.predecessor = None
        
        

    def serve(self) -> None:
        server = grpc.server(ThreadPoolExecutor(max_workers=2))
        chordprot_pb2_grpc.add_ChordServicer_to_server(self, server)
        server.add_insecure_port('[::]:50051')
        server.start()
        print('Server started')
        server.wait_for_termination()


    def join(self, request: JoinRequest, context) -> JoinResponse:
        
        if(request.init):
            
        else:
            return JoinResponse(num_hops=2)
    
if __name__ == '__main__':
    node = ChordNode()
    print(f"Ip address: {node.ip_addr}")
    for el in node.FT.FT:
        print(el)
    node.serve()
            
