import sqlite3
import os
import logging
from typing import List, Dict
from subprocess import (
    run, 
    CalledProcessError
)


class chordDb:
    '''
    Database management for each Chord node.

    This class handles the initialization and connection of the SQLite database used by each Chord node.
    It verifies the existence of a previous database -its name based on the node's hostname- and either 
    connects to it or creates a new one if none is found.

    Attributes:
        db_name(str): The name of the database file.
        connection(sqlite3.Connection): The SQLite database connection.
        cursor(sqlite3.Cursor): The SQLite database cursor.
    
    '''
    
    def __init__(self):
        '''
        __init__ 
        ========
        
        Initializes the chordDb object and establishes a connection to the database.

        This method initializes the chordDb object, configures the logger, and attempts to connect to the SQLite database.
        If a previous database file is found, it connects to it, otherwise leaves the connection stale. 
        It is then properly initialized by a call to write_disk(), when access to db is needed.
        It raises a `sqlite3.Error` if there is an issue during the database connection.

        Attributes:
            db_name(str): The name of the database file.
            connection(sqlite3.Connection): The SQLite database connection.
            cursor(sqlite3.Cursor): The SQLite database cursor.

        Raises:
            sqlite3.Error: If there is an error during the database connection.

        '''
        logging.basicConfig(level = logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        try:
            
            hostname = run("hostname -I", shell = True, capture_output = True, text = True).stdout.strip()
            self.db_name = f"{hostname}_chord.db"
            if os.path.exists(os.path.join("./Data", self.db_name)):
                self.logger.debug(f"Previous Db file found. Connecting to the database...")
                self.connection = sqlite3.connect(os.path.join("./Data", self.db_name), check_same_thread = False)
                self.cursor = self.connection.cursor()
            else:  
                self.logger.debug(f"Previous Db file not found. Creating the database...")
                self.connection = None
                self.cursor = None
                
            self.logger.debug(f"Successfully connected to the database.")
            
        except sqlite3.Error as error:
            self.logger.error(f"Error while connecting to the database: {error}")
    
    
    def write_disk(self) -> None:
        '''
        write_disk
        ==========
        
        This method establishes a new connection to the corresponding SQLite database
        and updates the 'connection' and 'cursor' attributes of the chordDb object
        to point to the appropriate database.
            
        Returns:
            None
        '''
        
        print(f"Entering write_disk method. Connecting to database...")
        self.connection = sqlite3.connect(os.path.join("./Data", self.db_name), check_same_thread = False)
        self.cursor = self.connection.cursor()
 

    def store_data(self, data_records)-> bool: 
        '''
        store_data
        ==========
        
        Stores data records in the SQLite database.

        This method creates a table named 'data_records' in the database if it doesn't exist.
        It then inserts the provided data records into the table.
        If the 'data_records' list is empty, a warning is logged, and the method returns True 
        storing naturally nothing in the database.
        
        Args:
            data_records (list): A list of dictionaries representing data records.
                                 Each dictionary should have keys: 'Surname', 'Education', 'Awards', and 'Hash'.
        
        Raises:
            sqlite3.Error: If there is an error during the database transaction.
            Exception: For other unexpected errors during data storage.
        
        Returns:
            bool: True if the data is successfully stored, False otherwise.

        '''

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        surname TEXT,
        education TEXT,
        awards INTEGER,
        hash_value INTEGER)
        ''')
        
        if len(data_records) == 0:
            self.logger.warning(f"No data to store in the database.")
            return True
        
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
        
    
    def fetch_data(self, education, awards_threshold = 0)-> List[Dict[str, any]]:
        '''
        fetch_data
        ==========
        
        Fetches data from the SQLite database based on the given university(eq -> education) and awards threshold(number of awards).

        This method retrieves records from the 'data_records' table in the SQLite database
        where the 'university' column matches the specified university and the 'awards' column
        is equal to or greater than the provided awards threshold.

        Args:
            education(str): The university to filter the records.
            awards_threshold(int, optional): The minimum number of awards required. Default is 0.
             
        Raises:
            sqlite3.Error: If there is an error during the database query.
        
        Returns:
            List[Dict[str, any]]: A list of dictionaries representing the fetched data records.
                                  If no matching records are found, an empty list is returned.
        
        '''
        
        try:
            print(f"Fetching data from database...")
            self.cursor.execute("SELECT surname, education, awards FROM data_records where education = ? and awards >= ?", (education, awards_threshold,))
            columns = [column[0] for column in self.cursor.description]
            data = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            self.logger.debug(f"Successfully fetched data from the database.")
            return data

        except sqlite3.Error as error:
            self.logger.error(f"Error while fetching data: {error}")
            return []
    
    
    def fetch_and_delete_data(self, threshold = None)-> List[Dict[str, any]]: #threshold is eq to the joining node hash value
        '''
        fetch_and_delete_data
        =====================
        
        Fetches and deletes data from the SQLite database based on a specified threshold.

        This method retrieves records from the 'data_records' table in the SQLite database
        based on the provided threshold. If the threshold parameter is None, it fetches and deletes all records
        hold in the corresponding node's database. In all other case, the method fetches and deletes records 
        having 'hash_value' less than or equal to the threshold.

        Args:
            threshold (int, optional): The hash value threshold. Default is None.
        
         Raises:
            sqlite3.Error: If there is an error during the database query or deletion.

        Returns:
            List[Dict[str, any]]: A list of dictionaries representing the fetched data records.
                                  If no matching records are found, an empty list is returned.
        
        '''
        
        if threshold is None: #eq the node leaves
            try:
                print(f"Fetching data from database...")
                self.cursor.execute("SELECT surname, education, awards, hash_value FROM data_records")
                columns = [column[0] for column in self.cursor.description]
                data = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
                self.logger.debug(f"Successfully fetched data from the database.")
                
                self.cursor.execute("DELETE FROM data_records")
                self.connection.commit()

                return data

            except sqlite3.Error as error:
                self.logger.error(f"Error while fetching and deleting data: {error}")
                self.connection.rollback()
                return []
            except Exception as e:
                self.logger.error(f"Error while fetching and deleting data: {e}")
                return []
        else: #eq a new node joins(new predecessor of current node)
            try:
                print(f"Fetching data from database...")
                self.cursor.execute("SELECT surname, education, awards, hash_value FROM data_records where hash_value <= ?", (threshold,))
                columns = [column[0] for column in self.cursor.description]
                data = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
                self.logger.debug(f"Successfully fetched data from the database.")
                
                try:
                    self.cursor.execute("DELETE FROM data_records where hash_value <= ?", (threshold,))
                    self.connection.commit()
                except sqlite3.Error as error:
                    self.logger.error(f"Error while deleting data: {error}")
                    self.connection.rollback()
                return data

            except sqlite3.Error as error:
                self.logger.error(f"Error while fetching and deleting data: {error}")
                return [] 
    
       