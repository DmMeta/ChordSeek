from concurrent.futures import ThreadPoolExecutor
import grpc

from chordprot_pb2 import (
    JoinResponse,
    JoinRequest,
    SuccessorRequest,
    SuccessorResponse,
    google_dot_protobuf_dot_empty__pb2 as google_protobuf_empty,
    setPredecessorRequest,
    FingerUpdateRequest
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
        hashed_ip_addr: int
        FT: List[Tuple[int, int, str]] = field(init=False)
        
        def __post_init__(self) -> None:
            self.__key__()

        def __key__(self) -> List[Tuple[int, int, str]]:
            ft_size = int(os.environ.get("FT_SIZE", 7))
            #careful loop arround chord with modulo when overflowing total the number of nodes 2^7.
            self.FT = [((self.hashed_ip_addr % (2**ft_size) + 2**i) % (2**ft_size) , 0, "") for i in range(ft_size)]
            
      

    def __init__(self) -> None:
        try: 
            result = run("hostname -I", shell = True, capture_output = True, text = True)
            self.ip_addr = result.stdout.strip()
        except CalledProcessError as e:
            print(f"Error occured: {e}")
        self.FT = self.FingerTable(hash(self.ip_addr))

        self.successor = ""
        self.predecessor = ""
        
        

    def serve(self) -> None:
        server = grpc.server(ThreadPoolExecutor(max_workers=2))
        chordprot_pb2_grpc.add_ChordServicer_to_server(self, server)
        server.add_insecure_port('[::]:50051')
        server.start()
        print('Server started')
        server.wait_for_termination()


    def join(self, request: JoinRequest, context) -> JoinResponse:
        
        print(request.ip_addr)
        if(request.init):
            ft_size = len(self.FT.FT) 
            for el in self.FT.FT:
                el = (el[0], self._own_key(), self.ip_addr)
            self.predecessor = self.ip_addr
            return JoinResponse(num_hops = 1)
        else:
            print(request.ip_addr)
            self.init_finger_table(request.ip_addr) # passing ip address
            self.update_others()
            return JoinResponse(num_hops = 2)

    def init_finger_table(self, ip_addr: str) -> None:
        #paper says finger[1].node | perhaps deadlock needs try catch
        key_id, ip_addr = self.find_successor(SuccessorRequest(key_id = self.FT.FT[0][0]))
        self.FT.FT[0] = (self.FT.FT[0][0],key_id, ip_addr)
        
        self.successor = self.FT.FT[0][2] 
        server = grpc.insecure_channel(self.successor+":50051")
        client = chordprot_pb2_grpc.ChordStub(server)
        
        self.predecessor =  client.get_predecessor().ip_addr #google_protobuf_empty the parameter ?
        client.set_predecessor(setPredecessorRequest(ip_addr = self.ip_addr))

        for i in range(len(self.FT.FT)-1):
            basic_condition =  self.FT.FT[i+1][0] in range(self._own_key(),self.FT.FT[i][1]) 
            #below condition checks if we rewinded in the chord network. 
            chord_rewind_condition = self._own_key() <= self.FT.FT[i+1][0] and self.FT.FT[i+1][0] > self.FT.FT[i][1]
            if ( basic_condition or chord_rewind_condition ):
                self.FT.FT[i+1][1], self.FT.FT[i][2] = self.FT.FT[i][1], self.FT.FT[i][2]
            else:
                server = grpc.insecure_channel(ip_addr+":50051")
                client = chordprot_pb2_grpc.ChordStub(server)
                self.FT.FT[i+1][1], self.FT.FT[i][2] = client.find_successor(SuccessorRequest(key_id = self.FT.FT[i+1][0]))

    def update_others(self) -> None:
        for i in range(len(self.FT.FT)):
            ip_addr = self.find_predecessor(self._own_key() - (2**i))
            server = grpc.insecure_channel(str(ip_addr)+":50051")
            client = chordprot_pb2_grpc.ChordStub(server)
            join_req = JoinRequest(ip_addr = self.ip_addr)
            client.update_finger_table(FingerUpdateRequest(join_rq = join_req, index = i))
    
    def update_finger_table(self, request, context) -> None:
        
        s = hash(request.join_rq.ip_addr) % (2**len(self.FT.FT))
        basic_condition =  s in range(self._own_key(), self.FT.FT[request.index][1]) 
        #below condition checks if we rewinded in the chord network. 
        chord_rewind_condition = self._own_key() <= s and s > self.FT.FT[request.index][1]
        if (basic_condition  or chord_rewind_condition):
            self.FT.FT[request.index][1], self.FT.FT[request.index][2] = s,request.join_rq.ip_addr
            p = self.predecessor
            server = grpc.insecure_channel(str(p)+":50051")
            client = chordprot_pb2_grpc.ChordStub(server)
            join_req = JoinRequest(ip_addr = s)
            client.update_finger_table(FingerUpdateRequest(join_rq = join_req, index = request.index))
            
        


    def find_successor(self, request:SuccessorRequest) -> tuple(int, str):
        print("here")
        print(request.key_id)
        pred_ip_addr = self.find_predecessor(request.key_id)

        server = grpc.insecure_channel(pred_ip_addr+":50051")
        client = chordprot_pb2_grpc.ChordStub(server)
        successor = client.get_successor() #google_protobuf_empty the parameter ?

        return successor.node_id, successor.ip_addr
        

    def find_predecessor(self, key_id: int) -> str:
        mirror_node = (self.ip_addr, self._own_key())
        
        server = grpc.insecure_channel(str(mirror_node[0])+":50051")
        client = chordprot_pb2_grpc.ChordStub(server)
        successor = client.get_successor() #google_protobuf_empty the parameter ?
  

        basic_condition =  key_id in range(mirror_node[1]+1, successor.node_id+1) 
        #below condition checks if we rewinded in the chord network. 
        #if it lies outside my jurisdiction but successor is due to rewind.
        chord_rewind_condition = mirror_node[1] < key_id and successor.node_id < mirror_node[1]

        #exclude call to different rpc method if the node it refers to is itself.
        if not (basic_condition or chord_rewind_condition):
            for i in range(len(self.FT.FT)-1,-1):
                if self.FT.FT[i][1] in range(mirror_node[1]+1, key_id):
                   mirror_node = (self.FT.FT[i][2], self.FT.FT[i][1])  


        while not (basic_condition or chord_rewind_condition):
            server = grpc.insecure_channel(str(mirror_node[0])+":50051")
            client = chordprot_pb2_grpc.ChordStub(server)
            mirror_node = client.closest_preceding_finger(SuccessorRequest(key_id = mirror_node[1])) #google_protobuf_empty the parameter ?
            
        return mirror_node[0] 

    def closest_preceding_finger(self, request: SuccessorRequest) -> SuccessorResponse:
        for i in range(len(self.FT.FT)-1,-1):
            if self.FT.FT[i][1] in range(self._own_key()+1, request.key_id):
                return SuccessorResponse(node_id = self.FT.FT[i][1], ip_addr = self.FT.FT[i][2])  
        return self._own_key(), self.ip_addr
            
     
        
    def get_successor(self) -> SuccessorResponse:
        return SuccessorResponse(node_id = self.FT.FT[0][1], ip_addr = self.FT.FT[0][2])
        

    def _own_key(self) -> int:
        return hash(self.ip_addr) % 2**len(self.FT.FT)


if __name__ == '__main__':
    node = ChordNode()
    print(f"Ip address: {node.ip_addr}")
    for el in node.FT.FT:
        print(el)
    node.serve()
            



