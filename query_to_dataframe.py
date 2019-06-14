'''
Author: Brian Mukeswe
Institution: The University of Edinburgh
Department: School of Informatics
Contact: b.mukeswe@sms.ed.ac.uk
Date: June 3, 2019

Description: This script can be used to retrieve into a pandas dataframe,
             the results of a query made to a mongodb collection.
'''

# import relevant libraries
import pandas as pd
from pymongo import MongoClient

def query_to_dataframe(query, collection):
    '''
    custom function to create a dataframe of query results
    from the weather_data
    '''
    entries = []
    for entry in collection.find(query):
        entries.append(entry)
      
    if len(entries)==0: # no hits for the query
        print("No hits for the specified query")
        return None
    else:
        return pd.DataFrame(entries).drop(columns=["_id"])

def config_collection(server):
    '''
    The pointer can then be used to add new documents to the collection, or
    to query objects from the collection.
   
    parameters:
    server: a dictionary containing a mongodb server configuration information
    
    return: a pointer to the specifed mongodb collection 
    '''
    client = MongoClient(host=server["host"], port=server["port"])
    db = client[server["database"]]
    collection = db[server["collection"]]

    return collection


# --------------------------------------------------------------------- #

## Configure the mongodb collection t be queried
server = {"host":"localhost",
          "port":27017,
          "database":"people",
          "collection":"default_collection"}

# Obtain collection pointer
collection = config_collection(server)


## Specify query (usingmongodb syntax)
query = {} # empty query returns all the documents in the collection


## Retireve query results in a dataframe
results = query_to_dataframe(query, collection)
