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


class WebCrawler:

    def __init__(self) -> None:
        with open(os.path.join('./','config.yml'), 'r') as config:
            config_file = yaml.load(config, Loader = yaml.FullLoader)
        
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
        
    def _get_scientists_(self) -> list[str]:
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
                        
            assert len(links) == len(names) ,"Found less than the expected number of scientists"
            
            return links, names
            
        except Exception as e:
            self.logger.error(f"Error occured: {e}")       
    
    def fetchData(self):
        nullInfobox = 0
        compsct_dict  = {}
        subpage = mwclient.Site('en.wikipedia.org')
        try: 
            for index, scientist in enumerate(self.scientists_links):
                
                scientist = scientist[scientist.find("/", 1)+1:]
                
                if index == 30:
                    return compsct_dict, nullInfobox
                
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
    
        parsed_info = {"alma_mater":[], "education":[], "awards":[]}
        #print(infobox)
        reg_ex = [r"\[\[(?!(?:PhD|Ms|Bs|Bsc|\|))([A-Z][^\[\]|]*)\]\]",r"\[\[(?!(?:PhD|Ms|Bs|Bsc|\|))([A-Z][^\[\]|]*)\]\]",r"\[\[(.*?)\]\]"]
        # reg_ex = [r'\[\[([^|\]]+)\]\]',r'\[\[([^|\]]+)\]\]',r"\[\[(.*?)\]\]"]
        #reg_ex = [r"\[\[(.*?)\]\]",r"\[\[(.*?)\]\]",r"\[\[(.*?)\]\]"]
        #test_regex = re.compile(    "\s*alma_mater\s*=[\s\S]*?([\.\=])")
        det= [r"\s*alma_mater\s*=([\s\S]*?([\.\=]))",]
        try :
            for index, detail in enumerate(details):
                if detail in infobox:
                    # parameter_start = infobox.index(detail)
                    # parameter_value_start = infobox.index("=", parameter_start) + 1
                    
                    # parameter_value_end = infobox.index(".", parameter_value_start)
                    
                    required_part = re.findall(f"\s*{detail}\s*=([\s\S]*?([\}}\=]))", infobox)
                    print(f"Infobox: {infobox}")
                    print(f"Required_part: {required_part}")
                    if len(list(chain(required_part))) != 0:
                        required_part = list(chain(required_part))[0][0] 
                    else: required_part = ""
                    #print(list(chain(required_part))[0][0],type(list(chain(required_part))[0][0]))
                   # \[\[([^\[\]|]+)\]\]
                   #r'\[\[([^|\]\d]+)(?:PhD|Ms|Bs|Bsc|\|\w+)?\]\]'
                    # print(required_part)
                    
                    parsed_info[detail] = re.findall(reg_ex[index], required_part,re.IGNORECASE)
            # \s*alma_mater\s*=\s*(.*?[^\=\.])   (\s*[\s\S]*) ([^=\n]*)  [\s\S]*?([\.\=])
            if len(parsed_info["alma_mater"]) == 0:
                del parsed_info["alma_mater"]
            else:
                parsed_info["education"] = parsed_info["alma_mater"]
                del parsed_info["alma_mater"]
                
        except Exception as e:
            self.logger.error(f"Error occured while parsing Infobox: {e}")
            
        return parsed_info
        


if __name__ == "__main__":
    test = WebCrawler()
    sttime = timer()
    dictionary, nullinfoBox = test.fetchData()
    endtime = timer()
    print(f"Elapsed time {endtime - sttime}")
    pp(dictionary)
    print(len(dictionary["Unknown University"]))
    print(nullinfoBox)

    with open(os.path.join('./','scientists.json'), 'w') as file:
        file.write(json.dumps(dictionary,indent=3))
    # print(len(test.scientists),test.scientists[:30])