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
import matplotlib.pyplot as plt

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
    print('The loaded file is: ' + dataFileName + ' with shape ' + str(dataframe.shape))
    
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

def suppress(dataframe, configDict):
    dataframe = dataframe.drop(columns = configDict['suppressCols'])
    print("Dropping columns from configuration file...")
    print("The shape of the new dataframe is:")
    print(dataframe.shape)
    return dataframe
    
def aggregateStats1(dataframe, configDict):
	#output - average speed of bus passing through the specific H3index, TimeSlot and sensitivity
	dataframe.to_csv('initdf.csv')

	#getting average speed for every license_plate in every HAT per day
	df = dataframe.groupby(['HAT','Date','license_plate']).agg({'speed':'mean'}).reset_index()
#	df.to_csv('test1.csv')
	
	#getting average of average speeds
	dfAgg = df.groupby('HAT').agg({'speed':'mean'}).reset_index()
#	dfAgg.to_csv('test2.csv')
	
	maxSpeed = dataframe['speed'].max()
	minSpeed = dataframe['speed'].min()
	
	#N is sum of number of unique license plates per HAT
	dfInter = dataframe.groupby(['HAT', 'Date']).agg({'license_plate':'nunique'}).reset_index()
#	dfInter.to_csv('test3.csv')
	dfInter = dfInter.groupby(['HAT']).agg({'license_plate':'sum'}).reset_index()
	dfAgg['N'] = dfInter['license_plate']
#	dfAgg.to_csv('test4.csv')
	

	#calculating local sensitivity (value for each unique HAT)
	localSensitivity = [None] * len(dfAgg)
	localSensitivity = (maxSpeed - minSpeed)/dfAgg['N']
	# dfAgg['localSensitivity'] = localSensitivity

	#calculating global sensitivity (1 value)
	date = dataframe["Date"].unique()
	threshold = configDict['minEventOccurences'] * len(date)
	globalSensitivity = (configDict['globalMaxSpeed'] - configDict['globalMinSpeed'])/dfAgg['N'].min()
	
	#checking type of user defined sensitivity from the config file
	sensitivityType = configDict['sensitivity']

	if sensitivityType == "local":
		sensitivity = localSensitivity
	else:
		sensitivity = globalSensitivity

	dfAgg['sensitivityFromConfig'] = sensitivity

	#finding 'K', the maximum number of HATs a bus passes through per day
	dfK = dataframe.groupby(['Date','license_plate']).agg({'HAT':'nunique'}).reset_index()
	K = dfK['HAT'].max()
#	print(K)

#	print(sensitivity)
	#remove after testing
	dfAgg.to_csv('test5.csv')
	return dfAgg, K
	
def variableNoiseAddition1(dataframe, privacyBudgetEps, K):
	#METHOD 1
	#calculating E' which is E/K where K is maximum number of HATs a bus passes through per day
	epsPrime = privacyBudgetEps/K
	
	#calculating noise 'b' for each HAT based on sensitivity using b = E/S
	dfVariableNoise = dataframe
	dfVariableNoise['b1'] = dfVariableNoise['sensitivityFromConfig']/epsPrime
	dfVariableNoise['b_method1'] = np.random.laplace(0,dfVariableNoise['b1'], len(dfVariableNoise))
	dfVariableNoise['NoisySpeed1'] = dfVariableNoise['speed'] + dfVariableNoise['b_method1']
	
	#METHOD 2	
	#calculating epsilon for b = 1, method applicable to global sensitivity
	eps_b1 = K * dfVariableNoise['sensitivityFromConfig'].max()
	# eps_b1 = dfVariableNoise['sensitivity'].sum()
	print(eps_b1)
	
	#computing required value of 'b' to meet the user-defined privacy budget
	b_method2 = eps_b1/privacyBudgetEps
	print(b_method2)
	# print('For the chosen privacy loss budget (Epsilon) of: ' + str(eps) + ', the noise \'b\' to be added is: ' + str(b))
	
	#adding noise 'b' to the average speed
	dfVariableNoise['b_method2'] = np.random.laplace(0,b_method2, len(dfVariableNoise))		
	dfVariableNoise['NoisySpeed2'] = dfVariableNoise['speed'] + dfVariableNoise['b_method2']
	dfVariableNoise.to_csv('dfVariable.csv')

	return
		
def noiseAddition1(dataframe, sensitivity, b, K):
#	epsMax = [None] * len(b)
#	cumulativeEps1 = [None]*len(b)
#	for i in range(len(b)):
#		dataframe['eps{:d}'.format(i)] = dataframe['sensitivity']/b[i]
#		epsMax[i] = dataframe['eps{:d}'.format(i)].max()
#		cumulativeEps1[i] = dataframe.nlargest(K,'eps{:d}'.format(i))
#	N_min = dataframe['N'].min()
#	print(N_min)
#	print(epsMax)
#	print(cumulativeEps1)
#	dataframe.to_csv('noiseTest.csv')
	return dataframe, epsMax, b
	
	
def aggregateStats2(dataframe, configDict):
	#output - average number of instances a bus passes through a HAT over the input speed limit
	#dropping all records lower than the chosen speedLimit
	speedLimit = configDict['speed_limit']
	dataframe = dataframe[(dataframe['speed'] > speedLimit)]
	
	#getting maximum speed for every license_plate in every HAT per day
	df = dataframe.groupby(['HAT','license_plate','Date']).agg({'speed':'max'}).reset_index()
	
	#remove after testing
#	df.to_csv('statsTest2.csv')
	
	#N is number of unique license plates per HAT that meet speed limit requirement
	dfAgg = df.groupby(['HAT']).agg({'license_plate':'nunique'}).reset_index()
	dfAgg.rename(columns = {'license_plate':'N'}, inplace = True)
#	print(dfAgg)
	
	#calculating the number of days in the dataset
	startDay = df['Date'].min()
	endDay = df['Date'].max()
	timeRange = (endDay - startDay).days
	
	#Calculating the average number of buses per day
	dfAgg['avgNumBuses'] = dfAgg['N']/timeRange
	
	#Calculating the sensitivity
	sensitivity = 1/timeRange
	
	#remove after testing
#	dfAgg.to_csv('statsTest3.csv')
	return dfAgg, sensitivity


def plot(x, y):
	plt.xlabel('B')
	plt.ylabel('Worst Case Epsilon')
	xValues = x
	yValues = y
	plt.plot(x,y, marker = 'x')
#	plt.legend()
	plt.show()
	return
	
	

	




