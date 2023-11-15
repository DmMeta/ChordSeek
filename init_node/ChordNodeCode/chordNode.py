from concurrent.futures import ThreadPoolExecutor
import grpc

from chordprot_pb2 import (
    JoinResponse,
    JoinRequest,
    SuccessorRequest,
    SuccessorResponse,
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
import logging
import hashlib
from itertools import chain
# from multiprocessing import Process
from time import sleep
import signal

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
            #careful loop around chord ring with modulo when overflowing the total number of possible key values(2^FT_SIZE).
            self.FT = [((self.hashed_ip_addr % (2**ft_size) + 2**i) % (2**ft_size) , 0, "") for i in range(ft_size)]
            
      

    def __init__(self) -> None:
        try: 
            result = run("hostname -I", shell = True, capture_output = True, text = True)
            self.ip_addr = result.stdout.strip()
        except CalledProcessError as e:
            print(f"Error  occured: {e}")
        self.FT = self.FingerTable(self._hash_(self.ip_addr))

        self.successor = ""
        self.predecessor = ""
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        

    def serve(self) -> None:
        server = grpc.server(ThreadPoolExecutor(max_workers=2))
        chordprot_pb2_grpc.add_ChordServicer_to_server(self, server)
        server.add_insecure_port('[::]:50051')
        server.start()
        print('Server started!')
        server.wait_for_termination()


    def join(self, request: JoinRequest, context) -> JoinResponse:
        if(request.init):
            self.logger.debug(f"Hash value of init_node: {self._own_key()}")
            ft_size = len(self.FT.FT) 
            for i in range(len(self.FT.FT)):
                self.FT.FT[i] = (self.FT.FT[i][0], self._own_key(), self.ip_addr)
            self.predecessor = self.ip_addr
            self.successor = self.ip_addr
            self.logger.debug(f"FingerTable of node after init_finger_table() {self.FT}")
            return JoinResponse(num_hops = 1)
        else:
            self.logger.debug(f"Hash value of joining_node: {self._own_key()}")
            print(request.ip_addr)
            self.init_finger_table(request.ip_addr) # passing ip address
            # if  not self.ip_addr == "10.0.0.32" and not  self.ip_addr == "10.0.0.31":
            self.logger.debug(f"Procceeding with the call to update_others!!!!!!!!!!!!")
            self.update_others()
            self.logger.debug(f"FingerTable of node after init_finger_table() {self.FT}")
            return JoinResponse(num_hops = 2)

    def init_finger_table(self, ip_addr: str) -> None:
        print(f"Node with hash value {self._own_key()} enters the init_finger_table().")
        try:
            with grpc.insecure_channel(ip_addr+":50051") as server:
                client = chordprot_pb2_grpc.ChordStub(server)
                self.logger.debug(f"finger[1].start : {self.FT.FT[0][0]}")
                successor = client.find_successor(SuccessorRequest(key_id = self.FT.FT[0][0]))
                key_id, succ_ip_addr = successor.node_id , successor.ip_addr
                self.logger.debug(f"returned  from find_successor: {key_id} -  {succ_ip_addr}")
                self.FT.FT[0] = (self.FT.FT[0][0], key_id, succ_ip_addr)

            self.successor = self.FT.FT[0][2] 
            self.logger.debug(f" here {self.successor} -  {self.FT.FT[0][2]}")
            with grpc.insecure_channel(self.successor+":50051") as server:
                client = chordprot_pb2_grpc.ChordStub(server)
                self.predecessor =  client.get_predecessor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()).ip_addr #google_protobuf_empty the parameter ?
                # Modification for first insertion into the network, otherwise deadlock.
                # if self.predecessor == self.successor:
                #     client.set_successor(setPredecessorRequest(ip_addr = self.ip_addr))
                client.set_predecessor(setPredecessorRequest(ip_addr = self.ip_addr))
                
            #Test-> setting the successor value of new_node's predecessor.
            with grpc.insecure_channel(self.predecessor+":50051") as server:
                client = chordprot_pb2_grpc.ChordStub(server)
                client.set_successor(setPredecessorRequest(ip_addr = self.ip_addr))
                
                self.logger.debug(f" Successor and predecessor were updated.")
                for i in range(len(self.FT.FT)-1):

                    
                    # basic_condition =  self.FT.FT[i+1][0] in range(self._own_key(),self.FT.FT[i][1]) 
                    # below condition checks if we rewinded in the chord network. 
                    # chord_rewind_condition_1 = self._own_key() <= self.FT.FT[i+1][0] and self.FT.FT[i+1][0] > self.FT.FT[i][1] and self._own_key() > self.FT.FT[i][1]
                    # chord_rewind_condition_2 = successor.node_id < self._own_key() and self._own_key() > self.FT.FT[i+1][0] and successor.node_id > self.FT.FT[i+1][0]
                    # if  basic_condition or chord_rewind_condition_1 or chord_rewind_condition_2:
                    
                    if self._in_between_(self._own_key(),self.FT.FT[i][1],self.FT.FT[i+1][0]):
                        self.FT.FT[i+1] = (self.FT.FT[i+1][0], self.FT.FT[i][1], self.FT.FT[i][2]) 
                    else:
                        with grpc.insecure_channel(ip_addr+":50051") as server:
                            client = chordprot_pb2_grpc.ChordStub(server)
                            successor = client.find_successor(SuccessorRequest(key_id = self.FT.FT[i+1][0]))
                            self.FT.FT[i+1] = (self.FT.FT[i+1][0], successor.node_id, successor.ip_addr)

            self.logger.debug(f"Init_finger_table reached its endpoint. Success\n Finger table: \n{self.FT}")
        except grpc.RpcError as e:
                self.logger.error(f"Error occured in init_finger_table: {e}")
        
            
    def update_others(self) -> None:
        for i in range(len(self.FT.FT)):
            self.logger.debug(f"Enters find_predecessor from update_others with key_id: {(self._own_key() - (2**i)) % (2**len(self.FT.FT))}") 
            ip_addr = self.find_predecessor((self._own_key() - (2**i) + 1) % (2**len(self.FT.FT))) #WARNING: the plus one solves the previous problem.
            self.logger.debug(f"Returned from find_predecessor inside update_others with: {ip_addr}\n join_req -> ip address: {self.ip_addr}") 
            server = grpc.insecure_channel(str(ip_addr)+":50051")
            client = chordprot_pb2_grpc.ChordStub(server)
            join_rq = JoinRequest(ip_addr = self.ip_addr)
            client.update_finger_table(FingerUpdateRequest(join_req = join_rq, index = i))
    
    def update_finger_table(self, request, context) :
        s = self._hash_(request.join_req.ip_addr) % (2**len(self.FT.FT))
        self.logger.debug(f"s is : {s}")
        # basic_condition =  s in range(self._own_key(), self.FT.FT[request.index][1]) 
        # below condition checks if we rewinded in the chord network. 
        # chord_rewind_condition_1 = self._own_key() <= s and s > self.FT.FT[request.index][1] #TODO: CHECK FOR POSSIBLE 3RD CONDITION
        # chord_rewind_condition_2 = s < self._own_key() and self._own_key() > self.FT.FT[request.index][1] and s < self.FT.FT[request.index][1]

        # (basic_condition or chord_rewind_condition_1) and not init_condition:
        init_condition = self._own_key() ==  s # and not init_condition
        #if not init_condition:
        if True:
            if self._own_key() == self.FT.FT[request.index][1]:
                if self._in_between_(self.FT.FT[request.index][0], self.FT.FT[request.index][1], s):
                    self.FT.FT[request.index] = (self.FT.FT[request.index][0], s, request.join_req.ip_addr) 
                    p = self.predecessor
                    server = grpc.insecure_channel(str(p)+":50051")
                    client = chordprot_pb2_grpc.ChordStub(server)
                    join_rq = JoinRequest(ip_addr = request.join_req.ip_addr)
                    self.logger.debug(f"Recursive call to update_finger_table on node: {request.join_req.ip_addr}")
                    client.update_finger_table(FingerUpdateRequest(join_req = join_rq, index = request.index))
            else :
                self.logger.debug(f"lowerbound: {self._own_key()}, s: {s}, upperbound: {self.FT.FT[request.index][1]} index: {request.index}")
                #WARNING: the plus one solves the problem of recursive calls.
                if self._in_between_(self._own_key() + 1, self.FT.FT[request.index][1] + 1, s):
                    self.logger.debug(f"update_finger_table else block inside if {s} in range ({self._own_key()},{self.FT.FT[request.index][1]})")
                    self.FT.FT[request.index] = (self.FT.FT[request.index][0], s, request.join_req.ip_addr) 
                    p = self.predecessor
                    server = grpc.insecure_channel(str(p)+":50051")
                    client = chordprot_pb2_grpc.ChordStub(server)
                    join_rq = JoinRequest(ip_addr = request.join_req.ip_addr)
                    self.logger.debug(f"Recursive call to update_finger_table on node: {request.join_req.ip_addr}")
                    client.update_finger_table(FingerUpdateRequest(join_req = join_rq, index = request.index))
                
        
        return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()
            
        


    def find_successor(self, request: SuccessorRequest, context) -> SuccessorResponse:
        try:
            self.logger.debug("here")
            self.logger.debug(request.key_id)
            pred_ip_addr = self.find_predecessor(request.key_id)
            self.logger.debug(f"The client that get_successor will be called onto: {pred_ip_addr}")
            with grpc.insecure_channel(pred_ip_addr+":50051") as channel:
                client = chordprot_pb2_grpc.ChordStub(channel)
                self.logger.debug(f"find_predecessor executes")
                successor = client.get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) #google_protobuf_empty the parameter ?
                self.logger.debug(f"inside find_successor: {type(successor.node_id)}{successor.node_id} - {type(successor.ip_addr)}{successor.ip_addr}")
                return SuccessorResponse(node_id = successor.node_id, ip_addr = successor.ip_addr)
        except grpc.RpcError as e:
            self.logger.error(f"Erro in find successor: {e}")
            
            

    def find_predecessor(self, key_id: int) -> str:
        #TODO: Probably change this rpc call to just an assignment.
        server = grpc.insecure_channel(self.ip_addr+":50051")
        client = chordprot_pb2_grpc.ChordStub(server)
        successor = client.get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) #google_protobuf_empty the parameter ?

        mirror_node = (self.ip_addr, self._own_key(), successor.node_id)
        mirror_node_copy =  tuple(mirror_node)

        self.logger.debug(f"{successor.node_id} <--> {mirror_node[1]}")
        #initial condition: If there is only one node in network
        if successor.node_id == mirror_node[1]:
         return mirror_node[0]

        # basic_condition =  key_id in range(mirror_node[1]+1, successor.node_id+1) 
        # #below condition checks if we rewinded in the chord network. 
        # #if it lies outside my jurisdiction but successor is due to rewind.
        # chord_rewind_condition_1 = mirror_node[1] < key_id and successor.node_id < mirror_node[1]
        # chord_rewind_condition_2 = (successor.node_id < mirror_node[1]) and (mirror_node[1] > key_id) and (successor.node_id > key_id)
         
        # #exclude call to different rpc method if the node it refers to is itself.
        # if not (basic_condition or chord_rewind_condition_1 or chord_rewind_condition_2):

        if not self._in_between_(mirror_node[1] + 1, successor.node_id + 1, key_id):
            for i in range(len(self.FT.FT)-1,-1,-1):
              if self._in_between_(self._own_key() + 1, key_id, self.FT.FT[i][1]):
                self.logger.debug(f"find_predecessor if block value: {self.FT.FT[i][1]}")
                mirror_node = (self.FT.FT[i][2], self.FT.FT[i][1])
                break
            # for i in range(len(self.FT.FT)-1,-1,-1):
            #     if(self._own_key() < key_id):
            #         if self.FT.FT[i][1] in range(mirror_node111[1]+1, key_id):
            #             mirror_node = (self.FT.FT[i][2], self.FT.FT[i][1])
            #             break
            #     else:
            #         if(self._own_key() != key_id):
            #             if self.FT.FT[i][1] in chain(range(self._own_key()+1, (2**len(self.FT.FT))+1), range(0, key_id)):
            #                 mirror_node = (self.FT.FT[i][2], self.FT.FT[i][1])
            #                 break  
        
        #to evade rpc calls inside loop which may have not been needed.
        if mirror_node_copy[1] != mirror_node[1]:
            with grpc.insecure_channel(str(mirror_node[0])+":50051") as channel:
                    client = chordprot_pb2_grpc.ChordStub(channel)
                    mirror_node_resp = client.get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 
                    self.logger.debug(f"mirror_node_resp from find_predecessor: {mirror_node_resp.node_id}")
                    mirror_node = (mirror_node[0], mirror_node[1], mirror_node_resp.node_id)                        


        #old conditions prio refactoring:
        # not key_id in range(mirror_node[1]+1, mirror_node[2]+1)  \
        # and not (mirror_node[1] < key_id and mirror_node[2] < mirror_node[1]) \
        # and not (mirror_node[2] < mirror_node[1] and mirror_node[1] > key_id and mirror_node[2] > key_id):



        while not self._in_between_(mirror_node[1] + 1, mirror_node[2]+1,key_id):
                server = grpc.insecure_channel(str(mirror_node[0])+":50051")
                client = chordprot_pb2_grpc.ChordStub(server)
                self.logger.debug(f"While block in find_predecessor with key_id: {key_id}")
                closest_preceding_finger_res = client.closest_preceding_finger(SuccessorRequest(key_id = key_id)) 
                self.logger.debug(f"closest_preceding_finger_res from find_predecessor: {closest_preceding_finger_res.ip_addr}")
                with grpc.insecure_channel(str(closest_preceding_finger_res.ip_addr)+":50051") as channel:
                    client = chordprot_pb2_grpc.ChordStub(channel)
                    mirror_node_succ = client.get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 
                    mirror_node = (closest_preceding_finger_res.ip_addr, closest_preceding_finger_res.node_id, mirror_node_succ.node_id)
                   
            
        return mirror_node[0] 

    def closest_preceding_finger(self, request: SuccessorRequest, context) -> SuccessorResponse:
        self.logger.debug(f"Enters closest_ preceding_finger with SuccessorRequest: {type(request.key_id)}{request.key_id}")
        for i in range(len(self.FT.FT)-1,-1,-1):
            if self._in_between_(self._own_key() + 1, request.key_id, self.FT.FT[i][1]):
                return SuccessorResponse(node_id = self.FT.FT[i][1], ip_addr = self.FT.FT[i][2])
            # if(self._own_key() < request.key_id):
            #  if self.FT.FT[i][1] in range(self._own_key()+1, request.key_id):
            #     return SuccessorResponse(node_id = self.FT.FT[i][1], ip_addr = self.FT.FT[i][2])  
            # else:
            #     if(self._own_key() != request.key_id):
            #       if self.FT.FT[i][1] in chain(range(self._own_key()+1, (2**len(self.FT.FT))+1), range(0, request.key_id)):
            #        return SuccessorResponse(node_id = self.FT.FT[i][1], ip_addr = self.FT.FT[i][2]) 
                    
        return SuccessorResponse(node_id = self._own_key(), ip_addr = self.ip_addr)
            
     
        
    def get_successor(self, request, context) -> SuccessorResponse:
        #return  SuccessorResponse(node_id = self._hash_(self.FT.FT[0][2]) % (2**len(self.FT.FT)) , ip_addr = self.FT.FT[0][2])
        return  SuccessorResponse(node_id = self._hash_(self.successor) % (2**len(self.FT.FT)) , ip_addr = self.successor)
    
    def get_predecessor(self, request, context) -> SuccessorResponse:
        return SuccessorResponse(node_id = self._hash_(self.predecessor) % 2**len(self.FT.FT), ip_addr = self.predecessor)
        
    def set_predecessor(self, request: setPredecessorRequest, context):
        self.predecessor = request.ip_addr
        return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()
    
    def set_successor(self, request: setPredecessorRequest, context):
        self.successor = request.ip_addr
        self.FT.FT[0] = (self.FT.FT[0][0], self._hash_(request.ip_addr) % 2**len(self.FT.FT), request.ip_addr)
        return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()

    
    def _in_between_(self,node_id_lobound,node_id_upbound,key_id):
        '''
        _in_between_
        =============
        node_id_lobound: lower bound of the range that is going to be checked, our RegionOfInterest(ROI)
        node_id_upbound: upper bound of the range that is going to be checked, our RegionOfInterest(ROI)
        key_id: value whose range is going to be checked.

        basic_condition -> if there is no rewind in chord key_id is checked if is in between node_id_lobound and node_id_upbound.
        We also need to check if rewind occured in the network. The first check in the `rewind_condition` evaluates the possible case where
        rewind occured. Next up, we check if key_id lies in the arc (of the chord) from the range(node_id_lobound,node_id_upbound).

        '''
        basic_condition = node_id_lobound < node_id_upbound and key_id in range(node_id_lobound,node_id_upbound) 
        rewind_condition = node_id_lobound > node_id_upbound and key_id in chain(range(node_id_lobound, 2**(len(self.FT.FT)) + 1), range(0,node_id_upbound))
        
        return basic_condition or rewind_condition

    def _hash_(self, data) -> int:
        sha256 = hashlib.sha256()
        sha256.update(data.encode('utf-8'))
        return int(sha256.hexdigest(),16)

    def _own_key(self) -> int:
        return self._hash_(self.ip_addr) % 2**len(self.FT.FT)


def print_fun(signum, frame):
    print(f"The final version of node's {node.ip_addr}(Hash value: {node._own_key()}) Finger Table(FT) is: \n{node.FT}")
    
#used global for debugging
node = ChordNode()
if __name__ == '__main__':
    print(f"{'=='*5}Starting Node  process {'=='*5}\nIp address: {node.ip_addr}")
    for el in node.FT.FT:
         print(el)
    print("====\n====")
    signal.signal(signal.SIGUSR1, print_fun)
    # printing_ft_process = Process(target = print_fun) 
    node.serve()
            



