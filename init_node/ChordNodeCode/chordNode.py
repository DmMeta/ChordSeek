from concurrent.futures import ThreadPoolExecutor
import grpc

from chordprot_pb2 import (
    JoinResponse,
    LeaveResponse,
    JoinRequest,
    SuccessorRequest,
    SuccessorResponse,
    setPredecessorRequest,
    FingerUpdateRequest,
    DataTransferRequest,
    JoiningNodeKeyRequest,
    DataTransferResponse,
    FixFingerRequest,
    CompScientistData
)


import chordprot_pb2_grpc
from dataclasses import dataclass, field
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
from google.protobuf.json_format import MessageToDict, Parse
from chordDb import chordDb

class ChordNode(chordprot_pb2_grpc.ChordServicer, chordprot_pb2_grpc.DataTransferServicer):
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

        self.successor = None
        self.predecessor = None
        self.chordDb = chordDb()
        self.stub = None
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
        server = grpc.server(ThreadPoolExecutor(max_workers=4))
        chordprot_pb2_grpc.add_ChordServicer_to_server(self, server)
        chordprot_pb2_grpc.add_DataTransferServicer_to_server(self,server)
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
            self.init_finger_table(request.ip_addr) # passing  ip address
            self.logger.debug(f"Finger Table(FT) of joining_node after init_finger_table(): {self.FT}")
            print(f"Predecessor of joining_node after init_finger_table(): IP Address -> {self.predecessor}, Hash Value -> {self._hash_(self.predecessor) % (2**len(self.FT.FT))}")
            print(f"Successor of joining_node after init_finger_table(): IP Address -> {self.successor}, Hash Value -> {self._hash_(self.successor) % (2**len(self.FT.FT))}")
            self.logger.debug(f"Proceeding with the call to update_others().")
            self.update_others()
            if(request.transfer_data):
              try:
                  self.chordDb.write_disk()
                  with grpc.insecure_channel(self.successor+":50051") as server:
                      client = chordprot_pb2_grpc.DataTransferStub(server)
                      node_data = client.request_data(JoiningNodeKeyRequest(node_id = self._own_key())) #TODO: implement transfer_data(). -> get from successor(grpc func) corresponding data_records(with condition: hash_value <= joining_node_hash_value)
                      node_data = MessageToDict(node_data, including_default_value_fields = True)
                      
                      if self.chordDb.store_data(node_data['data']):
                          self.logger.info(f"Success on transfering data from successor to joining node: {self._own_key()}.")
                      else:
                          raise grpc.RpcError(code = grpc.StatusCode.NOT_FOUND, details = "Error on storing data to joining node.")
                      
                      print(f"Successful completion of join().")    
              except grpc.RpcError as e:
                  self.logger.error(f"Error occured during the gRPC call: {e}")
              except Exception as e:
                  self.logger.error(f"Error occured: {e}")
            return JoinResponse(num_hops = 2)
          
    def leave(self,request, context) -> LeaveResponse:
      if self.predecessor == self._own_key and self._own_key == self.successor: #case1: the node that will leave is on its own in the network
        self.predecessor = None
        self.successor = None
        self.FT = None
        try:
          self.chordDb.fetch_and_delete_data()
        except  Exception as e:
          self.logger.error(f"An error occurred during the leave of node {self._own_key()}.")
        return LeaveResponse(num_hops = 5)
      
      else: 
        #case2: there are at least two nodes in the network
        # successor.predecessor = self.predecessor
        # predecessor.successor = self.successor
        # fetch_and_delete_data in node's db
        try:

          
          self.__establish_comm__(self.successor).set_predecessor(setPredecessorRequest(ip_addr = self.predecessor)) 
          self.__establish_comm__(self.predecessor).set_successor(setPredecessorRequest(ip_addr = self.successor)) 
          
          
        
          leaving_node_data = self.chordDb.fetch_and_delete_data()
          with grpc.insecure_channel(self.successor+":50051") as channel:
                     client = chordprot_pb2_grpc.DataTransferStub(channel)
                     dt = map(lambda scientist: CompScientistData(Surname = scientist.get("surname"),
                                                             Education = scientist.get("education"),
                                                             Awards = scientist.get("awards"),
                                                             Hash = scientist.get("hash_value")),leaving_node_data)

                     client.store(DataTransferRequest(data = dt)) #trans fer data from leaving node to leaving node's successor
          self.fix_others() # updating the finger tables of nodes  affected by the leave of current node
          self.successor = None
          self.predecessor = None
          self.FT = None
          print(f"Successful completion of leave().")
        
        except grpc.RpcError as e:
            self.logger.error(f"Error during transmission occured: {e}")   
        except  Exception as e:
          self.logger.error(f"An error occurred during the leave of node {self._own_key()}.")
        
        return LeaveResponse(num_hops = 8)
           
    def request_data(self, request: JoiningNodeKeyRequest, context) -> DataTransferResponse:
            try:
              joining_node_data = self.chordDb.fetch_and_delete_data(threshold = int(request.node_id))
              dt = map(lambda scientist: CompScientistData(Surname = scientist.get("surname"),
                                                             Education = scientist.get("education"),
                                                             Awards = scientist.get("awards"),
                                                             Hash = scientist.get("hash_value")),joining_node_data)
              return DataTransferResponse(data = dt)
            except  Exception as e:
                self.logger.error(f"Error occured during retrieval of joining node data: {e}")
                response = DataTransferResponse()
                return response

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
        
        print(f"Node {self._own_key()} enters the init_finger_table().")
        try:
            successor = self.__establish_comm__(ip_addr).find_successor(SuccessorRequest(key_id = self.FT.FT[0][0]))
            self.logger.debug(f"Finger[1].start : {self.FT.FT[0][0]}")
            key_id, succ_ip_addr = successor.node_id , successor.ip_addr
            print(f"Returned node from find_successor() call: {key_id} | {succ_ip_addr}.")
            self.FT.FT[0] = (self.FT.FT[0][0], key_id, succ_ip_addr)

            self.successor = self.FT.FT[0][2] 
            client = self.__establish_comm__(self.successor)
            self.predecessor =  client.get_predecessor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()).ip_addr
            client.set_predecessor(setPredecessorRequest(ip_addr = self.ip_addr))
                
            #print(f"Predecessor of node {self._own_key()} is: {self.predecessor}  -  {self._hash_(self.predecessor) % (2**len(self.FT.FT))}") 
            #print(f"Successor of node {self._own_key()} is: {self.successor} - {self._hash_(self.successor) % (2**len(self.FT.FT))}")      
                   
            self.__establish_comm__(self.predecessor).set_successor(setPredecessorRequest(ip_addr = self.ip_addr))
                
        
            for i in range(len(self.FT.FT)-1):
                if self._in_between_(self._own_key(),self.FT.FT[i][1],self.FT.FT[i+1][0]):
                    self.FT.FT[i+1] = (self.FT.FT[i+1][0], self.FT.FT[i][1], self.FT.FT[i][2]) 
                else:
                      successor = self.__establish_comm__(ip_addr).find_successor(SuccessorRequest(key_id = self.FT.FT[i+1][0]))
                      self.FT.FT[i+1] = (self.FT.FT[i+1][0], successor.node_id, successor.ip_addr)

            self.logger.debug(f"The execution of the init_finger_table function has been completed successfully.")
            
        except grpc.RpcError as e:
                self.logger.error(f"Error occured during the gRPC calls at init_finger_table(): {e}")
    
    
    def fix_others(self) -> None:
      
      for i in range(len(self.FT.FT)):
        print(f"Calling find_predecessor() from fix_others() with key_id: {(self._own_key() - (2**i)) % (2**len(self.FT.FT))}") 
        #WARNING: the plus one solves the previous problem.
        ip_addr = self.find_predecessor((self._own_key() - (2**i) + 1) % (2**len(self.FT.FT)))
        print(f"Returned node from find_predecessor(): {ip_addr} | {self._hash_(ip_addr) % (2**len(self.FT.FT))}")
        join_rq = JoinRequest(ip_addr = self.ip_addr)
        print(f"Calling fix_finger_table() from fix_others() on node {self._hash_(ip_addr) % (2**len(self.FT.FT))} with node_id value: {self._own_key()}")
        #tradeoff send bigger messages vs pay the find_successor call in fin_finger_table()
        self.__establish_comm__(ip_addr).fix_finger_table(FixFingerRequest(join_req = join_rq, successor_ip_addr = self.successor, index = i))  
    
    def fix_finger_table(self, request, context) -> chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty():
      s = self._hash_(request.join_req.ip_addr) % (2**len(self.FT.FT))
      successor_node_id = self._hash_(request.successor_ip_addr) % (2**len(self.FT.FT))
      print(f"Callee Node: {self._own_key()}, caller node: {request.join_req.ip_addr} | {s}, enters the fix_finger_table().")
      
      if self.FT.FT[request.index][1] == s:
        self.logger.debug(f"Finger[i].node updates its value from {self.FT.FT[request.index][1]} to { successor_node_id }.")
        self.FT.FT[request.index] = (self.FT.FT[request.index][0], successor_node_id, request.successor_ip_addr) 
        p = self.predecessor
        join_rq = JoinRequest(ip_addr = request.join_req.ip_addr)
        print(f"Recursive call to fix_finger_table on node {self._hash_(p) % (2**len(self.FT.FT))} with node_id value: {self._hash_(request.join_req.ip_addr) % (2**len(self.FT.FT))}")
        self.__establish_comm__(p).fix_finger_table(FixFingerRequest(join_req = join_rq, 
                                                 successor_ip_addr = request.successor_ip_addr, 
                                                 index = request.index))
      
      return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()
        
        
            
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
            join_rq = JoinRequest(ip_addr = self.ip_addr)
            print(f"Calling update_finger_table() from update_others() on node {self._hash_(ip_addr) % (2**len(self.FT.FT))} with node_id value: {self._own_key()}")
            self.__establish_comm__(ip_addr).update_finger_table(FingerUpdateRequest(join_req = join_rq, index = i))
            
            
    
    def update_finger_table(self, request, context) -> chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty():
        '''
        update_finger_table
        ===================
        
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
        print(f"Node {self._own_key()}, calling from node {request.join_req.ip_addr} | {s}, enters the update_finger_table().")
        print(f"Upper bound is: {self.FT.FT[request.index][1]}")
        #init_condition = self._own_key() ==  s # and not init_condition
        #if not init_condition:
        
        if self._own_key() == self.FT.FT[request.index][1]:
            if self._in_between_(self.FT.FT[request.index][0], self.FT.FT[request.index][1], s):
                self.logger.debug(f"Finger[i].node updates its value from {self.FT.FT[request.index][1]} to {s}.")
                self.FT.FT[request.index] = (self.FT.FT[request.index][0], s, request.join_req.ip_addr) 
                p = self.predecessor
                join_rq = JoinRequest(ip_addr = request.join_req.ip_addr)
                print(f"Recursive call to update_finger_table on node {self._hash_(p) % (2**len(self.FT.FT))} with node_id value: {self._hash_(request.join_req.ip_addr) % (2**len(self.FT.FT))}")
                self.__establish_comm__(p).update_finger_table(FingerUpdateRequest(join_req = join_rq, index = request.index))
        
        
        elif self._in_between_(self._own_key() + 1, self.FT.FT[request.index][1] + 1, s): #WARNING: the plus one at lbound solves the problem of recursive calls.
            self.logger.debug(f"Finger[i].node updates its value from {self.FT.FT[request.index][1]} to {s}.")
            self.FT.FT[request.index] = (self.FT.FT[request.index][0], s, request.join_req.ip_addr) 
            p = self.predecessor
            join_rq = JoinRequest(ip_addr = request.join_req.ip_addr)
            print(f"Recursive call to update_finger_table on node {self._hash_(p) % (2**len(self.FT.FT))} with node_id value: {self._hash_(request.join_req.ip_addr) % (2**len(self.FT.FT))}")
            self.__establish_comm__(p).update_finger_table(FingerUpdateRequest(join_req = join_rq, index = request.index))
                
        
        return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()
            
        


    def find_successor(self, request: SuccessorRequest, context) -> SuccessorResponse:
        '''
        find_successor
        ==============
        
         Finds the successor node for the given key_id using the Chord protocol algorithm.

         Args:
           request(SuccessorRequest): gRPC request containing the key_id for which the successor is to be found.
           context: The context of the gRPC communication.

         Raises:
           grpc.RpcError: An error that may occur during the gRPC communication.

         Note:
           This method calls the find_predecessor method to determine the predecessor node for the given key_id.
           It then establishes a gRPC channel with the predecessor node and retrieves the successor node using
           the get_successor method. The node_id and IP address of the successor node are returned in a
           SuccessorResponse object.
           
         Returns:
           SuccessorResponse: A response containing the node_id and IP address of the successor node.
      
        '''
        try:
            print(f"Calling find_predecessor() from find_successor() with key_id: {request.key_id}")
            pred_ip_addr = self.find_predecessor(request.key_id)
            print(f"Returned node from find_predecessor(): {pred_ip_addr} | {self._hash_(pred_ip_addr) % (2**len(self.FT.FT))}")
            successor = self.__establish_comm__(pred_ip_addr).get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 
            print(f"Returned successor node for key_id {request.key_id} is: {self._hash_(successor.ip_addr) % (2**len(self.FT.FT))}")
            return SuccessorResponse(node_id = successor.node_id, ip_addr = successor.ip_addr)
        except grpc.RpcError as e:
            self.logger.error(f"Error in find successor: {e}")
            
            

    def find_predecessor(self, key_id: int) -> str:
        '''
        find_predecessor
        ================
        
        Finds the predecessor node for the given key_id using the chord protocol algorithm.

        Args:
          key_id(int): The key_id for which the predecessor node is to be found.

        Note:
          This method utilizes the Chord Protocol to find the predecessor node for the specified key_id.
          It starts by retrieving the successor node and then iteratively finds the closest preceding finger
          until it identifies the predecessor. The process involves making gRPC calls to other nodes in the network.

          If the network has only one node, the method returns the current node's IP address as the predecessor.
       
        Returns:
          str: The IP address of the predecessor node.
        
        '''     
        successor = self.__establish_comm__(self.ip_addr).get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 

        mirror_node = (self.ip_addr, self._own_key(), successor.node_id)
        mirror_node_copy =  tuple(mirror_node)

        #initial condition: If there is only one node in network
        if successor.node_id == mirror_node[1]:
         return mirror_node[0]


        if not self._in_between_(mirror_node[1] + 1, successor.node_id + 1, key_id):
            print(f"Calling closest_preceding_finger() from find_predecessor() with key_id: {key_id}")
            for i in range(len(self.FT.FT)-1,-1,-1):
              if self._in_between_(self._own_key() + 1, key_id, self.FT.FT[i][1]):
                print(f"Closest preceding finger() returns {self.FT.FT[i][1]}")
                mirror_node = (self.FT.FT[i][2], self.FT.FT[i][1])
                break 
        
        #to evade rpc calls inside loop which may have not been needed.
        if mirror_node_copy[1] != mirror_node[1]:
                    mirror_node_resp = self.__establish_comm__(mirror_node[0]).get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 
                    print(f"Returned node from get_successor({mirror_node[1]}) is: {mirror_node_resp.node_id}")
                    mirror_node = (mirror_node[0], mirror_node[1], mirror_node_resp.node_id)                        


        while not self._in_between_(mirror_node[1] + 1, mirror_node[2] + 1, key_id):
                print(f"Calling closest_preceding_finger() from find_predecessor() with key_id: {key_id}")
                closest_preceding_finger_res = self.__establish_comm__(mirror_node[0]).closest_preceding_finger(SuccessorRequest(key_id = key_id)) 
                print(f"Closest preceding finger() returns {closest_preceding_finger_res.node_id}")

                mirror_node_succ = self.__establish_comm__(closest_preceding_finger_res.ip_addr).get_successor(chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()) 
                print(f"Returned node from get_successor({closest_preceding_finger_res.node_id}) is: {mirror_node_succ.node_id}")
                mirror_node = (closest_preceding_finger_res.ip_addr, closest_preceding_finger_res.node_id, mirror_node_succ.node_id)
                   
        self.logger.debug(f"The execution of find_predecessor() has been completed successfully.")    
        return mirror_node[0] 

    def closest_preceding_finger(self, request: SuccessorRequest, context) -> SuccessorResponse:
        '''
        closest_preceding_finger
        ========================
        
        Finds the closest preceding finger of current node to the specified key_id.

        Args:
          request(SuccessorRequest): gRPC request containing the key_id for which the closest preceding finger is to be found.
          context: The context of the gRPC communication.

        Note:
          This method used to identify the closest preceding finger to the specified key_id. 
          It iterates through the finger table in reverse order and returns the first finger that is in the range 
          (self._own_key() + 1, request.key_id).

          If no such finger is found, it returns the current node as the closest preceding finger.

        Returns:
          SuccessorResponse: A response containing the node_id and IP address of the closest preceding finger.

        '''
        print(f"Node {self._own_key()} enters the closest_preceding_finger() with key id {request.key_id}")
        for i in range(len(self.FT.FT)-1,-1,-1):
            if self._in_between_(self._own_key() + 1, request.key_id, self.FT.FT[i][1]):
                return SuccessorResponse(node_id = self.FT.FT[i][1], ip_addr = self.FT.FT[i][2])         
        return SuccessorResponse(node_id = self._own_key(), ip_addr = self.ip_addr)
            
     
        
    def get_successor(self, request, context) -> SuccessorResponse:
        '''
        get_successor
        =============
        
        Retrieves the successor node information.

        Args:
          request: An empty/unused gRPC request (== chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty).
          context: The context of the gRPC communication.
        
        Note:
          This method returns the information about the successor of the current node.
          The node_id is calculated based on the hash of the successor's IP address and is wrapped around
          the size of the finger table.
          
        Returns:
          SuccessorResponse: A response containing the node_id and IP address of the successor node.  
           
        '''
        return  SuccessorResponse(node_id = self._hash_(self.successor) % (2**len(self.FT.FT)) , ip_addr = self.successor)
    
    def get_predecessor(self, request, context) -> SuccessorResponse:
        '''
        get_predecessor
        ===============
        
        Retrieves the predecessor node information.

        Args:
          request: An empty/unused gRPC request (== chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty).
          context: The context of the gRPC communication.
        
        Note:
          This method returns the information about the predecessor of the current node.
          The node_id is calculated based on the hash of the predecessor's IP address and is wrapped around
          the size of the finger table.
          
        Returns:
          SuccessorResponse: A response containing the node_id and IP address of the predecessor node.
          
        '''
        return SuccessorResponse(node_id = self._hash_(self.predecessor) % 2**len(self.FT.FT), ip_addr = self.predecessor)
      
    def set_successor(self, request: setPredecessorRequest, context)-> chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty():
        '''
        set_successor
        =============
        
        Sets the successor node and updates the first entry of finger table.

        Args:
          request(setPredecessorRequest): gRPC request containing the IP address of the new successor.
          context: The context of the gRPC communication.
          
        Note:
          This method sets the successor node and updates the finger table to reflect the change.
          The new successor's IP address is stored in the 'successor' attribute and the finger table
          is updated accordingly. 
          
        Returns:
          chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty: An empty response.
        
        '''
        self.successor = request.ip_addr
        self.FT.FT[0] = (self.FT.FT[0][0], self._hash_(request.ip_addr) % 2**len(self.FT.FT), request.ip_addr)
        return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()
        
    def set_predecessor(self, request: setPredecessorRequest, context) -> chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty(): 
        '''
        set_predecessor
        =============
        
        Sets the predecessor node of current node.

        Args:
          request(setPredecessorRequest): gRPC request containing the IP address of the new predecessor.
          context: The context of the gRPC communication.
          
        Note:
          This method sets the predecessor node. The new successor's IP address is stored in the 'predecessor' attribute. 
          
        Returns:
          chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty: An empty response.
        
        '''  
        self.predecessor = request.ip_addr
        return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty() 
    
    
    
    def _in_between_(self, node_id_lobound, node_id_upbound, key_id) -> bool:
        '''
        _in_between_
        ============
        Checks if the key_id is in the range between the specified lower and upper bounds.

        Args:
          node_id_lobound: The lower bound of the range.
          node_id_upbound: The upper bound of the range.
          key_id: The key_id to be checked.
        
        Note:
          This method checks if the key_id falls within the range specified by the lower and upper bounds.
          The range is considered inclusive at the lower bound and exclusive at the upper bound.
          The method handles two cases: 
          1. Basic condition: The lower bound is less than the upper bound and the key_id is in the range.
          2. Rewind condition: The lower bound is greater than the upper bound and the key_id
          is in the range after considering the wrap-around of the identifier space.
      
        Returns:
          bool: True if the key_id is in the specified range, False otherwise.

        '''
        basic_condition = node_id_lobound < node_id_upbound and key_id in range(node_id_lobound,node_id_upbound) 
        rewind_condition = node_id_lobound > node_id_upbound and key_id in chain(range(node_id_lobound, 2**(len(self.FT.FT)) + 1), range(0,node_id_upbound))
        
        return basic_condition or rewind_condition

    def _hash_(self, data) -> int:
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
        return int(sha256.hexdigest(),16)

    def _own_key(self) -> int:
        '''
        _own_key
        ========
        
        Computes and returns the Chord key corresponding to the node's IP address.
        
        Note:
         This method computes the Chord key for the current node based on its IP address.
         It utilizes the `_hash_` method to generate a SHA-256 hash and then takes the modulo
         with the size of the identifier space defined by the finger table.

        Returns:
         int: The Chord key calculated from the node's IP address.
        
        '''
        return self._hash_(self.ip_addr) % 2**len(self.FT.FT)

    def store(self, request: DataTransferRequest, context)-> chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty():
      dict_repr = MessageToDict(request, including_default_value_fields = True)

      try:
        #create .dbs to disk so that you can store data.
        self.chordDb.write_disk()
        #careful pass a list of dictionaries containing the data
        if self.chordDb.store_data(dict_repr['data']): 
           context.set_code(grpc.StatusCode.OK)
           self.logger.info(f"Successfully stored data to node {self.ip_addr}")
        else: 
          context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
      except Exception as e:
          self.logger.error(f"Error while storing data: {e}")
      
      print(f"request: {dict_repr}") 
      return chordprot_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()
    
    def __establish_comm__(self, rpc_caller: str) -> None:
        channel = grpc.insecure_channel(str(rpc_caller)+":50051")
        self.stub = chordprot_pb2_grpc.ChordStub(channel)
        return self.stub

def print_fun(signum, frame) -> None:
    print(f"The final version of node's {node.ip_addr}(Hash value: {node._own_key()} | Predecessor: {node.predecessor}) Finger Table(FT) is: \n{node.FT}")
    
#used global for debugging
node = ChordNode()
if __name__ == '__main__':
    print(f"{'=='*5} Starting Node Process {'=='*5}\nIp address: {node.ip_addr}")
    print(f"Computed values of 'start' field at FT: ")
    for el in node.FT.FT:
         print(el)
    print("-----\n-----")
    signal.signal(signal.SIGUSR1, print_fun)
    # printing_ft_process = Process(target = print_fun) 
    node.serve()
            



