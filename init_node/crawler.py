import wikipediaapi
import re
import mwclient
from timeit import default_timer as timer
from beeprint import pp
import yaml
import os 
import logging
import json
from itertools import chain
import requests
from bs4 import BeautifulSoup
from urllib.parse import unquote
from typing import List

class WebCrawler:

    def __init__(self, config_file) -> None:
        
        
        self.EnglishAlphabetCardinality = config_file["crawler"]["englishalph_cardinality"]

        self.headers = {"User-Agent" :config_file["crawler"]["user_agent"]}
        self.page_title = config_file["crawler"]["page_title"]
        self.details = config_file["crawler"]["details"]
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level = logging.WARNING)
        try:  
            self.logger.info(f"Fetching subpages...")
            self.scientists_links , self.scientists_names = self._get_scientists_()
            self.logger.info(f"Done")
        except Exception as e:
            self.logger.error(f"Error occured: {e}")
        
    def _get_scientists_(self) -> List[str]:
        try:
            wiki_handle = wikipediaapi.Wikipedia('en', headers = self.headers)
            self.logger.info(f"Hitting {self.page_title} for data...")
            comp_scientists_page = wiki_handle.page(self.page_title)
        

            if not comp_scientists_page.exists():
                raise LookupError("Page does not exist")
            
           
            
            names = [re.split(r'\s[-–(]', scientist)[0].strip() 
                    for sec in comp_scientists_page.sections 
                    if re.search(r"Section: [A-Z] ", str(sec)) 
                    for scientist in sec.text.split("\n")]
            
            #edge case
            names[names.index("Admiral Grace Hopper")] = "Grace Hopper"
                
            response = requests.get(f"https://en.wikipedia.org/wiki/{self.page_title}")
            
            soup = BeautifulSoup(response.text, 'lxml')
            llinks = soup.select('a')
            links = [unquote(link['href']) for name in names for link in llinks if name in link]
            names = [unquote(link['title']) for name in names for link in llinks if name in link]            
            assert len(links) == len(names) ,"Found less than the expected number of scientists"
            
            return links, names
            
        except Exception as e:
            self.logger.error(f"Error occured: {e}")       
    
    def fetchData(self, batch_size = None):
        
        compsct_dict  = {}
        subpage = mwclient.Site('en.wikipedia.org')
        try: 
            for index, scientist in enumerate(self.scientists_links[:batch_size]):
                
                scientist = scientist[scientist.find("/", 1)+1:]
                
                
                print(f"[{index}]: Fetching scientist {scientist}...")
                infobox = subpage.pages[scientist].resolve_redirect().text(section = "Infobox")
                
                parsed_info = self._parseInfobox_(infobox, self.details)
                    #handle those with no university 
                if len(parsed_info["education"]) == 0:
                        if "Unknown University" not in compsct_dict.keys():
                            compsct_dict["Unknown University"] = []
                            
                        compsct_dict["Unknown University"] = [*compsct_dict["Unknown University"] , {
                                        "Surname": self.scientists_names[index],
                                        "Awards": len(parsed_info["awards"]),
                                        "Education": "Unknown University"
                                        }]   
     
                #handle those with the found university/ies
                for university in parsed_info["education"]:
                    if university not in compsct_dict.keys():
                        compsct_dict[university] = []

                    compsct_dict[university] = [*compsct_dict[university] , {
                                        "Surname": self.scientists_names[index],
                                        "Awards": len(parsed_info["awards"]),
                                        "Education": university
                                        }]    
                
            
            return compsct_dict 
        except Exception as e:
                self.logger.error(f"Error occured: {e}")

    def _parseInfobox_(self, infobox, details):
    
        parsed_info = {"alma_mater":[], "education":[], "awards":[], "prizes":[]}
        #print(infobox)
        reg_ex = [r"\[\[(?!(?:PhD|Ms|Bs|Bsc|\|))([A-Z][^\[\]|]*)\]\]",
                  r"\[\[(?!(?:PhD|Ms|Bs|Bsc|\|))([A-Z][^\[\]|]*)\]\]",
                  r"\[\[(.*?)\]\]", r"\[\[(.*?)\]\]"]  
        
        try :
            for index, detail in enumerate(details):
                if detail in infobox:
                    # parameter_start = infobox.index(detail)
                    # parameter_value_start = infobox.index("=", parameter_start) + 1
                    
                    # parameter_value_end = infobox.index(".", parameter_value_start)
                    
                    required_part = re.findall(f"\s*{detail}\s*=([\s\S]*?([\}}\=]))", infobox)
                    
                    # print(f"Infobox: {infobox}")
                    # print(f"Required_part: {required_part}")
                    if len(list(chain(required_part))) != 0:
                        required_part = list(chain(required_part))[0][0] 
                    else: required_part = ""
                    
                    
                    parsed_info[detail] = re.findall(reg_ex[index], required_part,re.IGNORECASE)
            
            if len(parsed_info["alma_mater"]) == 0:
                del parsed_info["alma_mater"]
            else:
                parsed_info["education"] = parsed_info["alma_mater"]
                del parsed_info["alma_mater"]
            
            parsed_info["awards"] = list(chain(parsed_info["awards"], parsed_info["prizes"]))
            del parsed_info["prizes"]
                
        except Exception as e:
            self.logger.error(f"Error occured while parsing Infobox: {e}")
            
        return parsed_info
        


if __name__ == "__main__":
    test = WebCrawler()
    sttime = timer()
    dictionary = test.fetchData()
    endtime = timer()
    print(f"Elapsed time {endtime - sttime}")
    pp(dictionary)
    print(len(dictionary["Unknown University"]))

    with open(os.path.join('./','scientists.json'), 'w', encoding='utf-8') as file:
        json.dump(dictionary, file, indent = 3)
    # print(len(test.scientists),test.scientists[:30])