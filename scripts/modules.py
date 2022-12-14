# File created to store modules for Differential Privacy
# Modules:
# 1. Filter/Suppress
# 2. Pseudonymization
# 3. Generalization
# 4. Aggregation
# 5. Differential Privacy (noise addition)
import pandas as pd
import numpy as np
import json
from pandas.io.json import json_normalize

def readFile(configFileName):
    #reading config
    configFile = '../config/' + configFileName
    with open(configFile, "r") as cfile:
        configDict = json.load(cfile)

    genType = configDict['genType']
    configDict = configDict['spatio-temporal']
    
    #reading datafile
    dataFileName = '../data/' + configDict['dataFile']
    with open(dataFileName, "r") as dfile:
        dataDict = json.load(dfile)
    #loading data
    dataframe = pd.json_normalize(dataDict)
    pd.set_option('mode.chained_assignment', None)
    print('The loaded file is: ' + dataFileName)
    
    #dropping duplicates based on config file parameters
    dupe1 = configDict['duplicateDetection'][0]
    dupe2 = configDict['duplicateDetection'][1]
    dfLen1 = len(dataframe)
    dfDrop = dataframe.drop_duplicates(subset = [dupe1, dupe2], inplace = False, ignore_index = True)
    dfLen2 = len(dfDrop)
    dupeCount = dfLen1 - dfLen2
    p1 = print(str(dupeCount) + ' duplicate rows have been removed.') 
    p2 = print(str(dfDrop.shape) + ' is the shape of the new dataframe.')
    dataframe = dfDrop  
    b = configDict['b']
    return dataframe, configDict, genType, b

def suppress(dataframe):
    dataframe = dataframe.drop(columns = configDict['suppressCols'])
    print("Dropping columns from configuration file...")
    print("The shape of the new dataframe is:")
    print(dataframe.shape)
    return dataframe
    
def aggregateStats1(dataframe):
	#output - average speed of bus passing through the specific H3index, TimeSlot and sensitivity
	
	#getting average speed for every license_plate in every HAT per day
	df = dataframe.groupby(['HAT','Date','license_plate']).agg({'speed':'mean'}).reset_index()

	#getting average of average speeds
	dfAgg = df.groupby('HAT').agg({'speed':'mean'}).reset_index()
	maxSpeed = dataframe['speed'].max()
	minSpeed = dataframe['speed'].min()
	
	#N is number of unique license plates per HAT
	N = dataframe.groupby(['HAT']).agg({'license_plate':'nunique'}).reset_index()

	#appending N to dfAgg
	dfAgg['N'] = N['license_plate']
	
	#calculating local sensitivity
	sensitivity = [None] * len(dfAgg)
	for i in range(len(dfAgg['N'])):
		sensitivity[i] = (maxSpeed-minSpeed)/dfAgg['N'][i]
	
	#assigning Sensitivity to dfAgg
	dfAgg['Sensitivity'] = sensitivity
	
	#remove after testing
	dfAgg.to_csv('sensTest.csv')
	return dfAgg, sensitivity

def aggregateStats2(dataframe, speedLimit):
	#output - average number of instances a bus passes through a HAT over the input speed limit
	#dropping all records lower than the chosen speedLimit
	dataframe = dataframe[(dataframe['speed'] > speedLimit)]
	
	#getting maximum speed for every license_plate in every HAT per day
	df = dataframe.groupby(['HAT','license_plate','Date']).agg({'speed':'max'}).reset_index()
	
	#remove after testing
	df.to_csv('sensTest2.csv')
	
	#N is number of unique license plates per HAT that meet speed limit requirement
	dfAgg = df.groupby(['HAT']).agg({'license_plate':'nunique'}).reset_index()
	dfAgg.rename(columns = {'license_plate':'N'}, inplace = True)
	print(dfAgg)
	
	#calculating the number of days in the dataset
	startDay = df['Date'].min()
	endDay = df['Date'].max()
	timeRange = (endDay - startDay).days
	
	#Calculating the average number of buses per day
	dfAgg['avgNumBuses'] = dfAgg['N']/timeRange
	
	#Calculating the sensitivity
	sensitivity = 1/timeRange
	
	#remove after testing
	dfAgg.to_csv('sensTest3.csv')
	return dfAgg, sensitivity
	
	
	
def plot(dataframe)"
	
	
	return
	
	
def noiseAddition(dataframe, sensitivity, b):
	for i in range(len(b)):
		ep[i] = sensitivity/(b[i])
		noise = np.random.laplace(0,b[i],1)
#    print("Epsilon= " + str(ep))
	return dataframe, noise, ep
	

#Define Epsilon as Sensitivity/b
#Add laplacian noise of b to the average speed
#Return the noisy average speed

	




