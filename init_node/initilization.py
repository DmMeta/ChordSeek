import os
import yaml
from crawler import WebCrawler
from __dataTransfer__ import DataTransfer
from __setup__ import ChordInitialization
from timeit import default_timer as timer
import hashlib
from functools import partial

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
    
        with open(os.path.join('./init_node','config.yml'), 'r') as config:
                    config_file = yaml.load(config, Loader = yaml.FullLoader)
        
        print(f"Beginning the setup of the chord network...")
        chord = ChordInitialization(config_file = config_file)
        chord.initialize()
        print(f"Network was successfully initialized")
        
        #check for previous file configuration
        Data_volume = os.listdir(os.path.join(config_file["chord"]["data_vol_path"]))
        dbs = [file for file in Data_volume if file.endswith(".db")]
        if len(dbs) == 0:
            crawler = WebCrawler(config_file = config_file)
            starttime = timer()
            Scientist_dict = crawler.fetchData()
            endtime = timer()
            print(f"Data successfully fetched in {endtime - starttime} seconds.")
            dataLoader = DataTransfer(data = Scientist_dict, network = chord.active_chord)
            dataLoader.transmitData(hash_fun = partial(hash, modulus = int(os.getenv("IDENT_SPACE_EXP"))))

        print(f"Initilization of the chord network was successful")
        
    
    except Exception as e:
        print(f"Error occured at Initilization: {e}")


if __name__ == "__main__":
    main()
    
    