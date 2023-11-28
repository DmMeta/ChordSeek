from grpc import ServerInterceptor


class HopsCounterInterceptor(ServerInterceptor):
    
    def __init__(self):
        self.hops = 0
        
    def intercept_service(self, continuation, handler_call_details):
        
        excluded_methods = ["get_successor", "set_successor", "get_predecessor", "set_predecessor", "get_data", "join",\
                            "leave", "request_data", "get_finger_table", "store","clear_hops"]

        excluded_methods = list(map(lambda method: f"/chordprot.Chord/{method}", excluded_methods))
        if handler_call_details.method not in excluded_methods:
            print(f"Method called: {handler_call_details.method}")
            self.hops += 1
        
        response =  continuation(handler_call_details)

        return response

    def reset_hops(self):
        self.hops = 0 
        print(f"Hops have been reset")