'''
Author: Brian Mukeswe
Institution: The University of Edinburgh
Department: School of Informatics
Contact: b.mukeswe@sms.ed.ac.uk
Date: June 3, 2019
'''

# import relevant libraries
import pandas as pd
from pymongo import MongoClient

def make_lineObj(line, data):
    ''' 
    create a dictionary object containing entries from a specified row
    of a dataframe.

    parameters:
    line - the row index of interest

    return:
    obj - a dictionary object containing the specified row entries
    '''
   
    obj = {}
    
    for column in data.columns:
        obj[column] = data[column].iloc[line]
        
    return obj


def storeObj(obj, collection):
    '''
    Store a dictionary object as a document in a specifed collection
    within a mongodb database

    parameters:
    obj - a dictionary object to be stored as a mongodb document
    collection - a pointer to a mongodb collection where the document 
                 will be sotred. The collection pointer can be obtained
                 from the fucntion config_collection(server)
    '''
    collection.insert_one(obj)


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

def store_data(data, collection):
    '''
    Stores all the rows in a pandas data frame as documents within a specified 
    collection on a mogodb database
    '''
    for row in range(0, len(data)):
        obj = make_lineObj(row, data)
        storeObj(obj, collection)



# --------------------------------------------------------------------------------#

## Configure mongodb database server

# modify server details as necessary
server = {"host" : "midgard09",
          "port" : 27017,
          "database" : "specify_data_base_name",
          "collection" : "specify_collection_name"}

# obtain collection pointer
collection = config_collection(server)


##  Specify the pandas data frame with the data to be stored. 

# I will use this example data here
example_data = [{"first_name" : "john",
                     "last_name" : "doe",
                     "age" : 20.0,
                     "email" : "johndoe@company.com"},
                    {"first_name" : "jane",
                     "last_name" : "doe",
                     "age" : 25.0,
                     "email" : "janedoe@company.com"}]
 
data = pd.DataFrame(example_data)

## Store data in the mongodb collection
store_data(data, collection)