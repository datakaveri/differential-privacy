# File created to store modules for Differential Privacy
# Modules:
# 1. Filter/Suppress
# 2. Generalization
# 3. Aggregation
# 4. Differential Privacy Computation (noise addition)
# 5. Post PostProcessing

#import statements
import pandas as pd
import numpy as np
import json
import jsonschema
from pandas.io.json import json_normalize
import matplotlib.pyplot as plt
import h3

def categorize(dataframe, configFile,genType):
    if genType == "spatio-temporal":
        dataframe = spatioTemporalGeneralization(dataframe, configFile)
    elif genType == "numeric":
        dataframe = numericGeneralization(dataframe)
    # TODO: introduce new categories
    return dataframe

def spatioTemporalGeneralization(dataframe, configFile):
    # separating latitude and longitude from location
    lat_lon = dataframe[configFile['locationCol']]
    split_lat_lon = lat_lon.astype(str).str.strip('[]').str.split(', ')
    lon = split_lat_lon.apply(lambda x: x[0])
    lat = split_lat_lon.apply(lambda x: x[1])

    #assigning h3 index to the latitude and longitude coordinates in separate dataframe  
    dfLen = len(dataframe)
    h3index = [None] * dfLen
    resolution = configFile["h3Resolution"]
    for i in range(dfLen):
        h3index[i] = h3.geo_to_h3(lat=float(lat[i]), lng=float(lon[i]), resolution=resolution)
    dataframe["h3index"] = h3index

    # assigning date and time to separate dataframe and creating a timeslot column
    dataframe["Date"] = pd.to_datetime(dataframe[configFile["datetimeCol"]]).dt.date
    dataframe["Time"] = pd.to_datetime(dataframe[configFile["datetimeCol"]]).dt.time
    time = dataframe["Time"]
    dataframe["Timeslot"] = time.apply(lambda x: x.hour)

    # assigning HATs from H3index and timeslot
    dataframe["HAT"] = ( dataframe["Timeslot"].astype(str) + " " + dataframe["h3index"])

    # Filtering time slots by start and end time from config file
    startTime = configFile["startTime"]
    endTime = configFile["endTime"]
    groupByColumn = configFile["groupByCol"]
    dataframe = dataframe[(dataframe["Timeslot"] >= startTime) & (dataframe["Timeslot"] <= endTime) ]

    # Selecting h3 indices where a min number of events occur in all timeslots of the day
    f1 = (dataframe.groupby(["Timeslot", "Date", "h3index"]).agg({groupByColumn: "nunique"}).reset_index())
    f2 = f1.groupby(["Timeslot", "h3index"]).agg({groupByColumn: "sum"}).reset_index()
    date = dataframe["Date"].unique()
    minEventOccurences = int(configFile["minEventOccurences"])
    limit = len(date) * minEventOccurences
    f3 = f2[f2[groupByColumn] >= limit]
    f4 = f3.groupby("h3index").agg({"Timeslot": "count"}).reset_index()

    maxTimeslots = f4["Timeslot"].max()
    f5 = f4[f4["Timeslot"] == maxTimeslots]

    df = dataframe["h3index"].isin(f5["h3index"])
    dataframe = dataframe[df]
    return dataframe

def numericGeneralization(dataframe, configFile):
    #TODO
    return dataframe

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

def readFile(configFileName):
    #reading config
    configFile = '../config/' + configFileName
    with open(configFile, "r") as cfile:
        configDict = json.load(cfile)
    
    #reading datafile
    dataFileName = '../data/' + configDict['dataFile']
    with open(dataFileName, "r") as dfile:
        dataDict = json.load(dfile)
    
    #loading data
    dataframe = pd.json_normalize(dataDict)
    pd.set_option('mode.chained_assignment', None)
    print('The loaded file is: ' + dataFileName + ' with shape ' + str(dataframe.shape))
    
    genType = configDict['genType']
    configDict = configDict['spatio-temporal']

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
    return dataframe, configDict, genType

def suppress(dataframe, configDict):
    dataframe = dataframe.drop(columns = configDict['suppressCols'])
    print("Dropping columns from configuration file...")
    print("The shape of the new dataframe is:")
    print(dataframe.shape)
    return dataframe
    
def aggregateStats1(dataframe, configDict):
    #output - average speed of bus passing through the specific H3index, TimeSlot and sensitivity

    #getting the column on which noise is to be added
    trueValue = configDict['trueValue']

    #calculating locality factor from the config file
    localityFactor = 1 + configDict['localityFactor']

    #getting average speed for every license_plate in every HAT per day
    df = dataframe.groupby(['HAT','Date','license_plate']).agg({trueValue:'mean'}).reset_index()
    
    #getting average of average speeds
    dfAgg = df.groupby('HAT').agg({trueValue:'mean'}).reset_index()
    
    #N is sum of number of unique license plates per HAT
    dfInter = dataframe.groupby(['HAT', 'Date']).agg({'license_plate':'nunique'}).reset_index()
    dfInter = dfInter.groupby(['HAT']).agg({'license_plate':'sum'}).reset_index()
    dfAgg['N'] = dfInter['license_plate']
    
    ############## DEPRECATED ################
    # calculating local sensitivity (value for each unique HAT)
    # localSensitivity = [None] * len(dfAgg)
    # localSensitivity = (maxSpeed - minSpeed)/dfAgg['N']
    # dfAgg['localSensitivity'] = localSensitivity
    ############## ########## ################

    #calculating global sensitivity (1 value)
    maxValue = configDict['globalMaxValue'] * localityFactor
    minValue = configDict['globalMinValue'] * localityFactor
    globalSensitivity = (maxValue - minValue)/dfAgg['N'].min()
    dfAgg['globalSensitivity'] = globalSensitivity

    #finding 'K', the maximum number of HATs a bus passes through per day and delocalising using locality factor
    dfK = dataframe.groupby(['Date','license_plate']).agg({'HAT':'nunique'}).reset_index()
    K = dfK['HAT'].max()
    K = K * localityFactor

    #remove after testing
    # dfAgg.to_csv('test5.csv')
    return dfAgg, K, maxValue, minValue
    
def variableNoiseAddition1(dataframe, configDict, K):
    
    #getting the column on which noise is to be added
    trueValue = configDict['trueValue']
    
    #calculating E' which is E/K where K is maximum number of HATs a bus passes through per day
    privacyLossBudgetEps = configDict['privacyLossBudgetEpsQuery'][0]
    epsPrime = privacyLossBudgetEps/K
    
    #calculating noise 'b' for each HAT based on sensitivity using b = S/E
    dfVariableNoise = dataframe
    globalSensitivity = dfVariableNoise['globalSensitivity'][0]
    b1 = globalSensitivity/epsPrime
    dfVariableNoise['b'] = np.random.laplace(0,b1, len(dfVariableNoise))
    dfVariableNoise['noisyValue'] = dfVariableNoise[trueValue] + dfVariableNoise['b']
    
    #remove after testing
    # dfVariableNoise.to_csv('NoisyDF.csv')

    #epsilon checker
    mapeThreshold = configDict['mapeThreshold'] 
    mean_absolute_percentage_error = np.mean(np.abs((dfVariableNoise[trueValue] - dfVariableNoise['noisyValue'])/dfVariableNoise[trueValue])) * 100
    print("MAPE for Query 1 is: " + str(mean_absolute_percentage_error))
    exitStatement = "The Privacy Loss Budget is too high! Please reduce the value in the Config file."
    if (mean_absolute_percentage_error <= mapeThreshold):
        return print(exitStatement), exit
    elif (mean_absolute_percentage_error > mapeThreshold):
        return dfVariableNoise, b1

def aggregateStats2(dataframe, configDict):
    #output - average number of instances per day a bus passes through a HAT over the input speed limit  
    
    #getting the column on which noise is to be added
    trueValue = configDict['trueValue']

    #calculating locality factor from the config file
    localityFactor = 1 + configDict['localityFactor']

    #dropping all records lower than the chosen speedLimit
    speedThreshold = configDict['trueValueThreshold']
    dataframeThreshold = dataframe[(dataframe[trueValue] > speedThreshold)]
    
    #getting maximum speed for every license_plate in every HAT per day
    df = dataframeThreshold.groupby(['HAT','license_plate','Date']).agg({trueValue:'max'}).reset_index()
    
    
    #N is number of unique license plates per HAT per day that exceed speed limit
    dfAgg = df.groupby(['HAT', 'Date']).agg({'license_plate':'nunique'}).reset_index()
    dfAgg.rename(columns = {'license_plate':'N'}, inplace = True)
    dfAgg = dfAgg.groupby(['HAT']).agg({'N':'sum'}).reset_index()


    #calculating the number of days in the dataset
    startDay = df['Date'].min()
    endDay = df['Date'].max()
    timeRange = 1 + (endDay - startDay).days
    
    #Calculating the average number of buses per day that exceed speed limit
    dfAgg['aggregateValue'] = dfAgg['N']/timeRange
    
    #remove after testing
    # dfAgg.to_csv('statsTest2.csv')
    
    #finding 'K', the maximum number of HATs a bus passes through per day and delocalising using locality factor
    dfK = dataframe.groupby(['Date','license_plate']).agg({'HAT':'nunique'}).reset_index()
    K = dfK['HAT'].max()
    K = K * localityFactor

    #global sensitivity as 1/No. of days
    globalSensitivity = 1/timeRange
    dfAgg['globalSensitivity'] = globalSensitivity

    #remove after testing
    # dfAgg.to_csv('statsTest3.csv')
    return dfAgg, timeRange

def variableNoiseAddition2(dataframe, configDict, K):
    #calculating E' which is E/K where K is maximum number of HATs a bus passes through per day
    privacyLossBudgetEps = configDict['privacyLossBudgetEpsQuery'][1]
    dfNoise = dataframe
    epsPrime = privacyLossBudgetEps/K

    #getting the sensitivity
    globalSensitivity = dfNoise['globalSensitivity'][0]

    #calculating the noise 'b' for each HAT based on sensitivity
    b2 = globalSensitivity/epsPrime
    # print(b2, epsPrime, globalSensitivity, K)
    dfNoise['b'] = np.random.laplace(0,b2,len(dfNoise))
    dfNoise['noisyValue'] = dfNoise['aggregateValue'] + dfNoise['b']
    dfNoise['noisyValue'] = np.round(dfNoise['noisyValue'])
    
    #remove after testing
    # dfNoise.to_csv('NoisyIncidents.csv')

    #epsilon checker 
    mapeThreshold = configDict['mapeThreshold']
    mean_absolute_percentage_error = np.mean(np.abs((dfNoise['aggregateValue'] - dfNoise['noisyValue'])/dfNoise['aggregateValue'])) * 100
    print("MAPE for Query 2 is: " + str(mean_absolute_percentage_error))
    exitStatement = "The Privacy Loss Budget is too high! Please reduce the value in the Config file."
    if (mean_absolute_percentage_error <= mapeThreshold):
        return print(exitStatement), exit
    elif (mean_absolute_percentage_error > mapeThreshold):
        return dfNoise, b2

def aggregator(dataframe, configDict):
    groupByCol = configDict['groupByCol']
    trueValueThreshold = configDict['trueValueThreshold']
    localityFactor = configDict['localityFactor']
    winsorizeLower = configDict['winsorizeLowerBound']
    winsorizeUpper = configDict['winsorizeUpperBound']

    # use to enforce a threshold
    # dfThreshold = dataframe[dataframe[groupByCol] >= trueValueThreshold]
    dfThreshold = dataframe

    #winsorizing the values of the chosen column
    lowClip = dfThreshold[groupByCol].quantile(winsorizeLower) * (1 - localityFactor)
    highClip = dfThreshold[groupByCol].quantile(winsorizeUpper) * (1 + localityFactor)
    dfThreshold[groupByCol].clip(lower=lowClip, upper=highClip, inplace = True)
        
    if (dfThreshold[groupByCol].dtype) == int or (dfThreshold[groupByCol].dtype) == float:
        dfGrouped = dfThreshold.groupby(['HAT','Date','license_plate']).agg(
                                count=(groupByCol,'count'),
                                sum=(groupByCol,'sum'),
                                mean=(groupByCol,'mean'),
                                max=(groupByCol,'max'),
                                min=(groupByCol,'min')).reset_index()
    else:
        dfGrouped = dfThreshold.groupby(['HAT']).agg(
                                count=(groupByCol,'count'))
        print('Warning: Only the count query is available for non-numeric choice of groupByCol')

    print(dfGrouped.head())
    # dfAggregated.to_csv('aggregatorTest.csv')
    return dfGrouped

def sensitivityCompute(dataframe, configDict):
    groupByCol = configDict['groupByCol']
    dfGrouped = dataframe

    #sensitivity for counting queries is always 1                                                                                    
    sensitivityCountQuery = 1

    #sensitivity for summation queries is diff(upper, lower) when bounded
    #calculating worst case sensitivity for all hats as global sensitivity
    max = dfGrouped['max'].max()
    min = dfGrouped['min'].min()
    sensitivitySummationQuery = max - min

    #sensitivity for mean queries


    sensitivity = np.array([sensitivityCountQuery, sensitivitySummationQuery])
    return sensitivity

def kCompute(dataframe, configDict):

    #finding 'K', the maximum number of HATs a bus passes through per day and delocalising using locality factor
    dfK = dataframe.groupby(['Date','license_plate']).agg({'HAT':'nunique'}).reset_index()
    K = dfK['HAT'].max()
    return K

def noiseCompute(dataframe, configDict, sensitivity, K):
    #sensitivity
    sensitivityCountQuery = sensitivity[0]
    sensitivitySummationQuery = sensitivity[1]

    privacyLossBudgetEps = configDict['privacyLossBudgetEpsQuery'][0]
    dfNoisy = dataframe
    epsPrime = privacyLossBudgetEps/K
    print('K:', K)
    #calculating the noise 'b' for each HAT based on sensitivity
    bCountQuery = sensitivityCountQuery/epsPrime
    bSummationQuery = sensitivitySummationQuery/epsPrime
    noiseCountQuery = np.random.laplace(0,bCountQuery,len(dfNoisy['count']))
    noiseSummationQuery = np.random.laplace(0,bSummationQuery,len(dfNoisy['sum']))

    dfNoisy['noisySum'] = dfNoisy['sum'] + noiseSummationQuery
    dfNoisy['noisyCount'] = dfNoisy['count'] + noiseCountQuery
    dfNoisy['noisyMean'] = dfNoisy['noisySum']/dfNoisy['noisyCount']
    # print(dfNoisy)
    return dfNoisy, noiseCountQuery, noiseSummationQuery

def postProcessing(dataframe, configDict, lowerClip = 0, upperClip = np.inf):
    #clipping upper and lower values to max and min used to define sensitivity
    dfNoisy = dataframe.drop(columns = ['license_plate', 'count', 'sum', 'mean', 'max', 'min'])
    
    dfNoisy['noisyCount'].clip(lowerClip, upperClip, inplace = True)
    dfNoisy['noisyCount'].round(0)
    # dfNoisy['noisySum'] = dataframe['noisyValue'].round(0)
    
    # creating the final dataframe
    dfFinal = dfNoisy[['HAT', 'Date', 'noisyCount']]
    print(dfFinal)
    return dfFinal

def signalToNoise(signal,noise):
    # calculate SNR
    # snr defined as signal mean over std of noise
    print(signal.mean(),noise)
    snr = (signal.mean())/(np.sqrt(2)*(noise))
    if snr <= 3:
        print("Your Signal to Noise Ratio of " + str(round(snr,3)) + " is within the acceptable bounds.")
    else:
        print("Your Signal to Noise Ratio of " + str(round(snr,3)) + " is quite high!")
    return snr
