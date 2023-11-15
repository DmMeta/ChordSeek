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
    A class that models the entity of a node in the Chord network. 
    The functionality includes behavior both as a client (sending requests) 
    and as a server (receiving requests).
    
    '''
    @dataclass
    class FingerTable:
        '''
        Data class modeling the finger table of a Chord network node.

        Attributes:
          hashed_ip_addr (int): The hashed IP address of the node.
          FT (List[Tuple[int, int, str]]): The finger table entries, each represented as a tuple.
        
        '''
        
        hashed_ip_addr: int
        FT: List[Tuple[int, int, str]] = field(init=False)
        
        def __post_init__(self) -> None:
            self.__key__()

        def __key__(self) -> List[Tuple[int, int, str]]:
            '''
            __key__
            =======
            
            Initialize the finger table based on the hashed IP address of the corresponding node.

            Returns:
              List[Tuple[int, int, str]]: The initialized finger table(FT), including 
              the correct values for the 'start' field of the entries in the FT.
            
            '''
            ft_size = int(os.environ.get("FT_SIZE", 7))
            #careful loop around chord ring with modulo when overflowing the total number of possible key values(2^FT_SIZE).
            self.FT = [((self.hashed_ip_addr % (2**ft_size) + 2**i) % (2**ft_size) , 0, "") for i in range(ft_size)]
            
      

    def __init__(self) -> None:
        '''
        __init__
        ========
        
        Initializes a Chord network node.
        
        Retrieves the IP address of the node by running the 'hostname -I' command.
        Initializes the finger table based on the hashed IP address(with the help of the nested DataClass -> FingerTable).
        Sets initial values for 'successor' and 'predecessor'.


        Attributes:
          ip_addr(str): The IP address of the node.
          FT(FingerTable): The finger table of the node.
          successor(str): The successor node in the Chord ring.
          predecessor(str): The predecessor node in the Chord ring.
        
        
        Note:
          In case of an error during the retrieval of the IP address, an exception is caught and
          an error message is printed.
          
        Returns:
          None
        
        '''
        try: 
            result = run("hostname -I", shell = True, capture_output = True, text = True)
            self.ip_addr = result.stdout.strip()
        except CalledProcessError as e:
            print(f"Error occured: {e}")
        self.FT = self.FingerTable(self._hash_(self.ip_addr))

        self.successor = ""
        self.predecessor = ""
        logging.basicConfig(level = logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        

    def serve(self) -> None:
        '''
        serve
        =====
        
        Starting the operation of the server - container and waiting for the (possible) incoming requests.
        
        Returns:
          None
          
        '''
        server = grpc.server(ThreadPoolExecutor(max_workers=2))
        chordprot_pb2_grpc.add_ChordServicer_to_server(self, server)
        server.add_insecure_port('[::]:50051')
        server.start()
        print(f"Server(IP Address: {self.ip_addr}) started!")
        server.wait_for_termination()


    def join(self, request: JoinRequest, context) -> JoinResponse:
        '''
        join
        ====
        
        Manages the process of integrating a node into the Chord ring network. 
        After its execution, the node introduced to the network has calculated the appropriate values for its finger table. 
        Additionally, in the case where the new node is not the first node (init_node)(to be introduced into the network), 
        necessary updates to the values of the finger tables of the nodes affected by the entry of the new node 
        are performed through a call to Update_others().

        Args:
          request (JoinRequest): gRPC request containing information(IP Address) about the network node from which 
          the initiation of the integration process of the new node into the network will begin. 
          Additionally, it includes a boolean variable init that determines whether the new node is the first one to be integrated into the network.
          
          context: The context object for the gRPC call.

        Returns:
          JoinResponse: The response indicating the success of the joining process and the required number of steps(number of hops) 
          needed for the integration of the node into the network.

        Note:
          If the 'init' flag is set to True in the request(indicating that the new node is the first one to be integrated into the network), 
          the node initializes its finger table and sets itself as the predecessor and successor. Otherwise, it initializes the finger table,
          updates its predecessor and successor and calls update_others() in order to 'notify' existing nodes about the new node.
          
        '''
        if(request.init):
            print(f"Hash value of init_node: {self._own_key()}")
            for i in range(len(self.FT.FT)):
                self.FT.FT[i] = (self.FT.FT[i][0], self._own_key(), self.ip_addr)
            self.predecessor = self.ip_addr
            self.successor = self.ip_addr
            self.logger.debug(f"Finger Table(FT) of init_node after init_finger_table(): \n {self.FT}")
            return JoinResponse(num_hops = 1)
        else:
            print(f"Hash value of joining_node: {self._own_key()}")
            self.init_finger_table(request.ip_addr) # passing ip address
            self.logger.debug(f"Finger Table(FT) of joining_node after init_finger_table(): {self.FT}")
            print(f"Predecessor of joining_node after init_finger_table(): IP Address -> {self.predecessor}, Hash Value -> {self._hash_(self.predecessor) % (2**len(self.FT.FT))}")
            print(f"Successor of joining_node after init_finger_table(): IP Address -> {self.successor}, Hash Value -> {self._hash_(self.successor) % (2**len(self.FT.FT))}")
            self.logger.debug(f"Proceeding with the call to update_others().")
            self.update_others()
            print(f"Completion of join().")
            return JoinResponse(num_hops = 2)
        

    def init_finger_table(self, ip_addr: str) -> None:
        '''
        init_finger_table
        =================
        
        Calculates the values of the node and node_ip_address fields of the finger table's entries
        by making gRPC calls to other nodes in the Chord network.

        Args:
          ip_addr (str): The IP address of the node to contact for starting the initialization of the finger table.

        Note:
          This method updates the finger table entries based on information retrieved from other nodes
          in the Chord network. It also updates the successor and predecessor information for the node.
          Additionally, it updates the successor of its predecessor and the predecessor of its successor.

        Raises:
          grpc.RpcError: If an error occurs during the gRPC calls.
          
        Returns:
          None

        '''
        
        print(f"Node with hash value -> {self._own_key()} | IP address -> {self.ip_addr} enters the init_finger_table().")
        try:
            with grpc.insecure_channel(ip_addr+":50051") as server:
                client = chordprot_pb2_grpc.ChordStub(server)
                self.logger.debug(f"Finger[1].start : {self.FT.FT[0][0]}")
                successor = client.find_successor(SuccessorRequest(key_id = self.FT.FT[0][0]))
                key_id, succ_ip_addr = successor.node_id , successor.ip_addr
                self.logger.debug(f"Returned node from find_successor() call: {key_id} | {succ_ip_addr}.")
                self.FT.FT[0] = (self.FT.FT[0][0], key_id, succ_ip_addr)

            self.successor = self.FT.FT[0][2] 
            with grpc.insecure_channel(self.successor+":50051") as server:
                client = chordprot_pb2_grpc.ChordStub(server)
                self.predecessor =  client.get_predecessor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()).ip_addr
                client.set_predecessor(setPredecessorRequest(ip_addr = self.ip_addr))
                
            #print(f"Predecessor of node {self._own_key()} is: {self.predecessor} - {self._hash_(self.predecessor) % (2**len(self.FT.FT))}") 
            #print(f"Successor of node {self._own_key()} is: {self.successor} - {self._hash_(self.successor) % (2**len(self.FT.FT))}")      
                   
            with grpc.insecure_channel(self.predecessor+":50051") as server:
                client = chordprot_pb2_grpc.ChordStub(server)
                client.set_successor(setPredecessorRequest(ip_addr = self.ip_addr))
                
        
            for i in range(len(self.FT.FT)-1):
                if self._in_between_(self._own_key(),self.FT.FT[i][1],self.FT.FT[i+1][0]):
                    self.FT.FT[i+1] = (self.FT.FT[i+1][0], self.FT.FT[i][1], self.FT.FT[i][2]) 
                else:
                    with grpc.insecure_channel(ip_addr+":50051") as server:
                        client = chordprot_pb2_grpc.ChordStub(server)
                        successor = client.find_successor(SuccessorRequest(key_id = self.FT.FT[i+1][0]))
                        self.FT.FT[i+1] = (self.FT.FT[i+1][0], successor.node_id, successor.ip_addr)

            self.logger.debug(f"The execution of the init_finger_table function has been completed successfully.")
            
        except grpc.RpcError as e:
                self.logger.error(f"Error occured during the gRPC calls at init_finger_table(): {e}")
        
            
    def update_others(self) -> None:
        '''
        update_others
        =============
        
        Initiates the process of updating the finger tables of other nodes in the Chord network affected by the entry of a new node.

        This method iterates through the finger table entries of the current node and makes gRPC calls
        to the appropriate predecessor nodes of those entries in order to update their finger tables. 
        The goal is to propagate the influence of the new node's entry throughout the network, 
        ensuring consistency in the finger tables of other nodes that may be affected.
        
        Note:
          The entry of the new node can potentially change the value of the .node | .node_ip_address(successor information) field in the finger tables 
          of other nodes in the network. As a consequence, node n will need to be entered into the finger tables of some existing nodes. 
          The general rule is:
          -> Node n will become the ith finger.node|.node_ip_address value of node k iff
             {k precedes n by at least 2^(i-1) AND the ith finger.node value on node k succeeds n} 

        Returns:
          None
        
        '''
        for i in range(len(self.FT.FT)):
            print(f"Calling find_predecessor() from update_others() with key_id: {(self._own_key() - (2**i)) % (2**len(self.FT.FT))}") 
            #WARNING: the plus one solves the previous problem.
            ip_addr = self.find_predecessor((self._own_key() - (2**i) + 1) % (2**len(self.FT.FT)))
            print(f"Returned node from find_predecessor(): {ip_addr} | {self._hash_(ip_addr) % (2**len(self.FT.FT))}") 
            server = grpc.insecure_channel(str(ip_addr)+":50051")
            client = chordprot_pb2_grpc.ChordStub(server)
            join_rq = JoinRequest(ip_addr = self.ip_addr)
            print(f"Calling update_finger_table() from update_others() on node {self._hash_(ip_addr) % (2**len(self.FT.FT))} with node_id value: {self._own_key()}")
            client.update_finger_table(FingerUpdateRequest(join_req = join_rq, index = i))
            
            
    
    def update_finger_table(self, request, context) -> chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty():
        '''
        update_finger_table
        =============
        
        Updates the finger table entry of the corresponding node based on a gRPC request from other node.

        Args:
          request: gRPC request containing the IP address of the node that made the request and whose value 
          will be checked to determine whether a change in the finger table is needed. It also contains the index of the 
          corresponding entry in the finger table.
          context: The gRPC context.

        Note:
          This method is called recursively to ensure the consistency of the finger table across the Chord network.
          It evaluates the appropriate conditions to determine if an update is necessary and makes appropriate updates to the
          finger table entry. The recursion continues until the conditions are satisfied or the appropriate finger
          table entry is updated.

        
        Returns:
          chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty: An empty response.
        
        '''
        s = self._hash_(request.join_req.ip_addr) % (2**len(self.FT.FT))
        print(f"Node with hash value -> {self._own_key()} | IP address -> {self.ip_addr}, calling from node {request.join_req.ip_addr} | {s}, enters the update_finger_table().")
        print(f"Upper bound is: {self.FT.FT[request.index][1]}")
        #init_condition = self._own_key() ==  s # and not init_condition
        #if not init_condition:
        
        if self._own_key() == self.FT.FT[request.index][1]:
            if self._in_between_(self.FT.FT[request.index][0], self.FT.FT[request.index][1], s):
                self.logger.debug(f"Finger[i].node updates its value from {self.FT.FT[request.index][1]} to {s}.")
                self.FT.FT[request.index] = (self.FT.FT[request.index][0], s, request.join_req.ip_addr) 
                p = self.predecessor
                server = grpc.insecure_channel(str(p)+":50051")
                client = chordprot_pb2_grpc.ChordStub(server)
                join_rq = JoinRequest(ip_addr = request.join_req.ip_addr)
                print(f"Recursive call to update_finger_table on node {self._hash_(p) % (2**len(self.FT.FT))} with node_id value: {self._hash_(request.join_req.ip_addr) % (2**len(self.FT.FT))}")
                client.update_finger_table(FingerUpdateRequest(join_req = join_rq, index = request.index))
        
        
        elif self._in_between_(self._own_key() + 1, self.FT.FT[request.index][1] + 1, s): #WARNING: the plus one at lbound solves the problem of recursive calls.
            self.logger.debug(f"Finger[i].node updates its value from {self.FT.FT[request.index][1]} to {s}.")
            self.FT.FT[request.index] = (self.FT.FT[request.index][0], s, request.join_req.ip_addr) 
            p = self.predecessor
            server = grpc.insecure_channel(str(p)+":50051")
            client = chordprot_pb2_grpc.ChordStub(server)
            join_rq = JoinRequest(ip_addr = request.join_req.ip_addr)
            print(f"Recursive call to update_finger_table on node {self._hash_(p) % (2**len(self.FT.FT))} with node_id value: {self._hash_(request.join_req.ip_addr) % (2**len(self.FT.FT))}")
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
        server = grpc.insecure_channel(self.ip_addr+":50051")
        client = chordprot_pb2_grpc.ChordStub(server)
        successor = client.get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) #google_protobuf_empty the parameter ?

        mirror_node = (self.ip_addr, self._own_key(), successor.node_id)
        mirror_node_copy =  tuple(mirror_node)

        self.logger.debug(f"{successor.node_id} <--> {mirror_node[1]}")
        #initial condition: If there is only one node in network
        if successor.node_id == mirror_node[1]:
         return mirror_node[0]


        if not self._in_between_(mirror_node[1] + 1, successor.node_id + 1, key_id):
            for i in range(len(self.FT.FT)-1,-1,-1):
              if self._in_between_(self._own_key() + 1, key_id, self.FT.FT[i][1]):
                self.logger.debug(f"find_predecessor if block value: {self.FT.FT[i][1]}")
                mirror_node = (self.FT.FT[i][2], self.FT.FT[i][1])
                break 
        
        #to evade rpc calls inside loop which may have not been needed.
        if mirror_node_copy[1] != mirror_node[1]:
            with grpc.insecure_channel(str(mirror_node[0])+":50051") as channel:
                    client = chordprot_pb2_grpc.ChordStub(channel)
                    mirror_node_resp = client.get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 
                    self.logger.debug(f"mirror_node_resp from find_predecessor: {mirror_node_resp.node_id}")
                    mirror_node = (mirror_node[0], mirror_node[1], mirror_node_resp.node_id)                        


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
        return SuccessorResponse(node_id = self._own_key(), ip_addr = self.ip_addr)
            
     
        
    def get_successor(self, request, context) -> SuccessorResponse:
        #return  SuccessorResponse(node_id = self._hash_(self.FT.FT[0][2]) % (2**len(self.FT.FT)) , ip_addr = self.FT.FT[0][2])
        return  SuccessorResponse(node_id = self._hash_(self.successor) % (2**len(self.FT.FT)) , ip_addr = self.successor)
    
    def get_predecessor(self, request, context) -> SuccessorResponse:
        return SuccessorResponse(node_id = self._hash_(self.predecessor) % 2**len(self.FT.FT), ip_addr = self.predecessor)
        
    def set_predecessor(self, request: setPredecessorRequest, context) -> chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty(): 
        self.predecessor = request.ip_addr
        return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty() 
    
    def set_successor(self, request: setPredecessorRequest, context)-> chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty():
        self.successor = request.ip_addr
        self.FT.FT[0] = (self.FT.FT[0][0], self._hash_(request.ip_addr) % 2**len(self.FT.FT), request.ip_addr)
        return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()

    
    def _in_between_(self,node_id_lobound,node_id_upbound,key_id) -> bool:
        '''
        _in_between_
        ============
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


def print_fun(signum, frame) -> None:
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
            



