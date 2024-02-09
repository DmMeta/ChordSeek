import os
import yaml
from crawler import WebCrawler
from __dataTransfer__ import DataTransfer
from __setup__ import (
    ChordInitialization,
    ChordStub,
    grpc,
    JoinRequest,
    google_pb_empty
)
from timeit import default_timer as timer
import hashlib
from functools import partial

from random import randint

def hash(data, modulus) -> int:
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
        return int(sha256.hexdigest(),16) % (2**modulus)  




def main():
    
    try:
    
        with open(os.path.join('netcrwl_config.yml'), 'r') as config:
                    config_file = yaml.load(config, Loader = yaml.FullLoader)
        
        print(f"Beginning the setup of the chord network...")
        chord = ChordInitialization(config_file = config_file)
        chord.initialize()
        print(f"Network was successfully initialized.")
        
        
        cache_hit = os.environ.get(config_file['data_cache']) 
        print(cache_hit)
        if cache_hit == "miss":
            crawler = WebCrawler(config_file = config_file)
            starttime = timer()
            Scientist_dict = crawler.fetchData()
            endtime = timer()
            print(f"Data successfully fetched in {endtime - starttime} seconds.")
            dataLoader = DataTransfer(data = Scientist_dict, network = chord.active_chord)
            dataLoader.transmitData(hash_fun = partial(hash, modulus = int(os.environ.get(config_file['chord']['exp']))))

        print(f"Initilization of the chord network was successful.")

      
    except Exception as e:
        print(f"Error occured at Initilization: {e}")


if __name__ == "__main__":
    main()
    
    