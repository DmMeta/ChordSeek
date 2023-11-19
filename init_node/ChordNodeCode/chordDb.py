import sqlite3
import os
import logging
from typing import List, Dict
from subprocess import (
    run, 
    CalledProcessError
)


class chordDb:
    
    def __init__(self):
        
        logging.basicConfig(level = logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        try:
            
            hostname = run("hostname -I", shell = True, capture_output = True, text = True).stdout.strip()
            self.db_name = f"{hostname}_chord.db"
            if os.path.exists(os.path.join("./Data", self.db_name)):
                self.logger.debug(f"Previous Db file found. Connecting to the database...")
                self.connection = sqlite3.connect(os.path.join("./Data", self.db_name))
                self.cursor = self.connection.cursor()
            else:  
                self.logger.debug(f"Previous Db file not found. Creating the database...")
                self.connection = None
                self.cursor = None
                
            self.logger.debug(f"Successfully connected to the database.")
            
        except sqlite3.Error as error:
            self.logger.error(f"Error while connecting to the database: {error}")
 


    def store_data(self, data_records)-> bool: #maybe unecessary?
        
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        surname TEXT,
        education TEXT,
        awards INTEGER,
        hash_value INTEGER)
        ''')
        
        try:
          
          for record in data_records:
            self.cursor.execute("INSERT INTO data_records (surname, education, awards, hash_value) VALUES (?, ?, ?, ?)",\
                              (record['Surname'], record['Education'], record['Awards'], record['Hash']))

          self.connection.commit()
          self.logger.debug(f"Successfully stored data in the database.")
          return True
        except sqlite3.Error as error:
            self.logger.error(f"Error while storing data: {error}")
            self.connection.rollback() #rollbacks the transaction if an error occurs.
            return False
        except Exception as e:
            self.logger.error(f"Error while storing data: {e}")
            return False
    
    def write_disk(self) -> None:
        self.connection = sqlite3.connect(os.path.join("./Data", self.db_name))
        self.cursor = self.connection.cursor()
        
    
    def fetch_data(self, education, awards_threshold = 0)-> List[Dict[str, any]]:
        try:
            self.cursor.execute("SELECT surname, education, awards FROM data_records where education =? and awards >=?", (education, awards_threshold))
            columns = [column[0] for column in self.cursor.description]
            data = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return data

        except sqlite3.Error as error:
            self.logger.error(f"Error while fetching data: {error}")
            return []
    
    
    def fetch_and_delete_data(self, threshold = None)-> List[Dict[str, any]]: #threshold is eq to the joining node hash value
        if threshold is None: #eq the node leaves
            try:
                self.cursor.execute("SELECT surname, education, awards, hash_value FROM data_records")
                columns = [column[0] for column in self.cursor.description]
                data = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
                try:
                    self.cursor.execute("DELETE FROM data_records")
                    self.connection.commit()
                except sqlite3.Error as error:
                    self.logger.error(f"Error while deleting data: {error}")
                    self.connection.rollback()
                    return data

            except sqlite3.Error as error:
                self.logger.error(f"Error while fetching and deleting data: {error}")
                return []
            except Exception as e:
                self.logger.error(f"Error while fetching and deleting data: {e}")
                return []
        else: #eq a new node joins(new predecessor of current node)
            try:
                self.cursor.execute("SELECT surname, education, awards, hash_value FROM data_records where hash_value <=?", (threshold))
                columns = [column[0] for column in self.cursor.description]
                data = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
                try:
                    self.cursor.execute("DELETE FROM data_records where hash_value <=?", (threshold))
                    self.connection.commit()
                except sqlite3.Error as error:
                    self.logger.error(f"Error while deleting data: {error}")
                    self.connection.rollback()
                return data

            except sqlite3.Error as error:
                self.logger.error(f"Error while fetching and deleting data: {error}")
                return [] 
       