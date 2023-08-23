import json
import jsonschema
import pandas as pd
from pandas import json_normalize
import random
import string

def schemaValidator(schemaFile, configFile):
    schemaFile = '../config/' + schemaFile
    configFile = '../config/' + configFile

# Load the JSON schema
    with open(schemaFile, "r") as f:
        schema = json.load(f)

# Load the JSON document to validate
    with open(configFile, "r") as f:
        document = json.load(f)

# Validate the document against the schema
    jsonschema.validate(instance=document, schema=schema)
    return

def dropDuplicates(dataframe, configDict):

    #dropping duplicates based on config file parameters
    if(len(configDict['duplicateDetection']))==0:
        dfLen1 = len(dataframe)
        dfDrop = dataframe.drop_duplicates(inplace = False, ignore_index = True)
        dfLen2 = len(dfDrop)
        dupeCount = dfLen1 - dfLen2
        print("\nIdentifying and removing duplicates...")
        print(str(dupeCount) + ' duplicate rows have been removed.') 
        print(str(dfDrop.shape) + ' is the shape of the deduplicated dataframe .')
        dataframe = dfDrop  
    else:
        dfLen1 = len(dataframe)
        dfDrop = dataframe.drop_duplicates(subset = configDict['duplicateDetection'], inplace = False, ignore_index = True)
        dfLen2 = len(dfDrop)
        dupeCount = dfLen1 - dfLen2
        print("\nIdentifying and removing duplicates...")
        print(str(dupeCount) + ' duplicate rows have been removed.') 
        print(str(dfDrop.shape) + ' is the shape of the deduplicated dataframe .')
        dataframe = dfDrop  
    return dataframe

def suppress(dataframe, configDict):
    dataframe = dataframe.drop(columns = configDict['suppressCols'])
    print("\nDropping columns from configuration file...")
    print(str(dataframe.shape) + ' is the shape of the dataframe after suppression.\n\nThe number of unique rows are:\n' + str(dataframe.shape[0]))
    return dataframe

def pseudonymize(df, configDict):
    dataframe = df.copy()
    pseudoCol = configDict['pseudoCol']
    for col in pseudoCol:
        uniquePlates = dataframe[col].unique()
        pseudonymizedCol = {}
        for item in uniquePlates:
            item = str(item)
            pseudonymizedItem = ''
            for char in item:
                if char.isalpha():
                    random_char = random.choice(string.ascii_uppercase)
                    pseudonymizedItem += random_char
                elif char.isdigit():
                    random_char = random.choice(string.digits)
                    pseudonymizedItem += random_char
                else:
                    pseudonymizedItem += char
            pseudonymizedCol[item] = pseudonymizedItem
        # dataframe.drop([pseudoCol], axis = 1, inplace = True)
        dataframe[col] = dataframe[col].map(pseudonymizedCol)
    return dataframe

