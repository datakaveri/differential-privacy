    
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
    print('\nNumber of unique HATs created is: ' + str(dataframe['HAT'].nunique()))

    # Filtering time slots by start and end time from config file
    startTime = configFile["startTime"]
    endTime = configFile["endTime"]
    groupByColumn = 'license_plate'
    dataframe = dataframe[(dataframe["Timeslot"] >= startTime) & (dataframe["Timeslot"] <= endTime) ]

    # Selecting h3 indices where a min number of events occur in all timeslots of the day
    df1 = (dataframe.groupby(["HAT", "Date"]).agg({groupByColumn: "nunique"}).reset_index())
    df2 = df1.groupby(["HAT"]).agg({groupByColumn: "sum"}).reset_index()

    #filtering average num of occurences per day per HAT
    date = dataframe["Date"].unique()
    minEventOccurencesPerDay = int(configFile["minEventOccurences"])
    limit = len(date) * minEventOccurencesPerDay
    df3 = df2[df2[groupByColumn] >= limit]
    df = dataframe["HAT"].isin(df3["HAT"])
    dataframe = dataframe[df]

    print('Number of unique HATs left after filtering is: ' + str(dataframe['HAT'].nunique()))

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
    dataFileName = '../data/' + configDict['dataFile']
    
    if configDict['genType']=='spatio-temporal':        
        #reading datafile
        
        with open(dataFileName, "r") as dfile:
            dataDict = json.load(dfile)        
        #loading data
        dataframe = pd.json_normalize(dataDict)
        
    elif configDict['genType']=='categorical':
        dataframe = pd.read_json(dataFileName)
        
    pd.set_option('mode.chained_assignment', None)
    print('The loaded file is: ' + dataFileName + ' with shape ' + str(dataframe.shape))
    
    genType = configDict['genType']
    configDict = configDict[genType]
    
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
        #subset= []
        #for i in range(len(configDict['duplicateDetection'])):
        #    subset = subset.append(configDict['duplicateDetection'][i])
        #print(subset)
        #dupe1 = configDict['duplicateDetection'][0]
        #dupe2 = configDict['duplicateDetection'][1]
        dfLen1 = len(dataframe)
        dfDrop = dataframe.drop_duplicates(subset = configDict['duplicateDetection'], inplace = False, ignore_index = True)
        dfLen2 = len(dfDrop)
        dupeCount = dfLen1 - dfLen2
        print("\nIdentifying and removing duplicates...")
        print(str(dupeCount) + ' duplicate rows have been removed.') 
        print(str(dfDrop.shape) + ' is the shape of the deduplicated dataframe .')
        dataframe = dfDrop  
    
    return dataframe, configDict, genType

def suppress(dataframe, configDict):
    dataframe = dataframe.drop(columns = configDict['suppressCols'])
    print("\nDropping columns from configuration file...")
    print(str(dataframe.shape) + ' is the shape of the dataframe after suppression.\n\nThe number of unique rows are:\n' + str(dataframe.shape[0]))
    return dataframe
    
def timeRange(dataframe):
    #calculating the number of days in the dataset
    startDay = dataframe['Date'].min()
    endDay = dataframe['Date'].max()
    timeRange = 1 + (endDay - startDay).days
    return timeRange

def aggregator(dataframe, configDict):
    #initializing variables from config file
    groupByCol = configDict['groupByCol']
    localityFactor = configDict['localityFactor']
    winsorizeLower = configDict['winsorizeLowerBound']
    winsorizeUpper = configDict['winsorizeUpperBound']
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
        
        aggFunctionCount = {groupByCol: ['count']}
        dfSensitivity = dfThreshold.groupby(['HAT', 'license_plate', 'Date']).agg(aggFunctionCount)
        dfSensitivity.columns = dfSensitivity.columns.droplevel(0)
        dfSensitivity.reset_index(inplace = True)
        # print(dfSensitivity)

        # dfCount = dfSensitivity.groupby(['HAT'], as_index=False).agg(['max', 'sum'])
        dfCount = dfSensitivity.groupby(['HAT']).agg(
                                max=('count', 'max'),
                                sum=('count', 'sum'))
        # dfCount.columns = dfCount.columns.droplevel(0)
        dfCount.reset_index(inplace = True)
        # print(dfCount['max'])

    else:
        dfGrouped = dfThreshold.groupby(['HAT']).agg(
                                count=(groupByCol,'count'))
        print('Warning: Only the count query is available for non-numeric choice of groupByCol')

    return dfGrouped, dfSensitivity, dfCount

def ITMSQuery1(dataframe):
    #average speed per HAT
    dfITMSQuery1 = dataframe

    #getting average of average speeds
    dfITMSQuery1 = dfITMSQuery1.groupby('HAT').agg({'mean':'mean'}).reset_index()
    dfITMSQuery1.rename(columns = {'mean':'queryOutput'}, inplace = True)
    # print(dfITMSQuery1)
    return dfITMSQuery1

def ITMSQuery1Weighted(dataframe):
    dfITMSQuery1Weighted = dataframe
    #weighted mean
    dfITMSQuery1WeightedHATSum = dfITMSQuery1Weighted.groupby('HAT').agg({'sum':'sum'})
    dfITMSQuery1WeightedHATCount = dfITMSQuery1Weighted.groupby('HAT').agg({'count':'sum'})
    dfITMSQuery1Weighted = dfITMSQuery1WeightedHATSum['sum']/dfITMSQuery1WeightedHATCount['count']
    dfITMSQuery1Weighted = dfITMSQuery1Weighted.to_frame().reset_index()
    # dfITMSQuery1Weighted.rename(columns = {'':'queryOutput'}, inplace = True)
    # //TODO convert series to dataframe
    # print(dfITMSQuery1Weighted)
    return dfITMSQuery1Weighted

def ITMSQuery2(dataframe, configDict):
    #average number of speed violations per HAT over all days

    #dropping all records lower than the chosen speedLimit
    speedThreshold = configDict['trueValueThreshold']

    # dropping all rows that don't meet threshold requirement
    dfITMSQuery2 = dataframe[(dataframe['max'] >= speedThreshold)].reset_index()

    # finding number of threshold violations per HAT, per Day, per license plate
    dfITMSQuery2 = dfITMSQuery2.groupby(['HAT', 'Date']).agg({'license_plate':'count'}).reset_index()

    # finding average number of violations per HAT over all the days
    dfITMSQuery2 = dfITMSQuery2.groupby(['HAT']).agg({'license_plate':'mean'}).reset_index()
    dfITMSQuery2.rename(columns={'license_plate':'queryOutput'}, inplace = True)
    return dfITMSQuery2

def NCompute(dataframe):
    #N is sum of number of unique license plates per HAT
    dataframe = dataframe.groupby(['HAT', 'Date']).agg({'license_plate':'nunique'}).reset_index()
    dataframe = dataframe.groupby(['HAT']).agg({'license_plate':'sum'}).reset_index()
    dataframe.rename(columns={'license_plate':'N'}, inplace = True)
    dfN = dataframe
    # print(dfN)
    #since 'n' is the denominator in sensitivity, max change in sensitivity is from min value of 'n'
    N = dataframe['N'].min()
    return N, dfN

def KCompute(dataframe):
    #finding 'K', the maximum number of HATs a bus passes through per day
    dfK = dataframe.groupby(['Date','license_plate']).agg({'HAT':'nunique'}).reset_index()
    K = dfK['HAT'].max()
    return K

def ITMSSensitivityCompute(configDict, timeRange, N, dfN, dfSensitivity, dfCount):
    maxValue = configDict['globalMaxValue']
    minValue = configDict['globalMinValue']

    # sensitivity for query 1
    # print(dfN)
    sensitivityITMSQuery1 = ((maxValue - minValue)/dfN['N'])
    # print(sensitivityITMSQuery1)
    
    # sensitivity for weighted query 1
    sensitivityITMSQuery1Weighted = ((dfCount['max']*(maxValue - minValue))/(dfCount['sum']))
    # print(sensitivityITMSQuery1Weighted)

    # sensitivity for query 2
    sensitivityITMSQuery2 = 1/timeRange

    return sensitivityITMSQuery1, sensitivityITMSQuery2, sensitivityITMSQuery1Weighted

def noiseComputeITMSQuery(dfITMSQuery1, dfITMSQuery2, dfITMSQuery1Weighted, sensitivityITMSQuery1, sensitivityITMSQuery2, sensitivityITMSQuery1Weighted, configDict, K):
    dfNoiseITMSQuery1 = dfITMSQuery1
    dfNoiseITMSQuery2 = dfITMSQuery2
    dfNoiseITMSQuery1Weighted = dfITMSQuery1Weighted

    # epsilon
    privacyLossBudgetEpsITMSQuery1 = configDict['privacyLossBudgetEpsQuery'][0]
    privacyLossBudgetEpsITMSQuery2 = configDict['privacyLossBudgetEpsQuery'][1]

    # computing epsilon prime
    epsPrimeQuery1 = privacyLossBudgetEpsITMSQuery1/K
    epsPrimeQuery2 = privacyLossBudgetEpsITMSQuery2/K

    # computing noise query 1
    bITMSQuery1 = sensitivityITMSQuery1/epsPrimeQuery1
    # print(sensitivityITMSQuery1)
    # print(bITMSQuery1)
    noiseITMSQuery1 = np.random.laplace(0, bITMSQuery1)
    # print(len(noiseITMSQuery1))
    # print(noiseITMSQuery1)
    # print(np.linalg.norm(noiseITMSQuery1, ord = 2))


    # computing noise weighted query 1
    bITMSQuery1Weighted = sensitivityITMSQuery1Weighted/epsPrimeQuery1
    noiseITMSQuery1Weighted = np.random.laplace(0, bITMSQuery1Weighted)
    print(noiseITMSQuery1Weighted)
    # print(np.linalg.norm(noiseITMSQuery1Weighted, ord = 2))

    # computing noise query 2
    bITMSQuery2 = sensitivityITMSQuery2/epsPrimeQuery2
    noiseITMSQuery2 = np.random.laplace(0, bITMSQuery2, len(dfNoiseITMSQuery2))

    # adding noise to the true value
    dfNoiseITMSQuery1['queryNoisyOutput'] = dfNoiseITMSQuery1['queryOutput'] + noiseITMSQuery1
    dfNoiseITMSQuery2['queryNoisyOutput'] = dfNoiseITMSQuery2['queryOutput'] + noiseITMSQuery2
    # dfNoiseITMSQuery1Weighted['queryNoisyOutput'] = dfNoiseITMSQuery1Weighted['queryOutput'] + noiseITMSQuery1Weighted

    return dfNoiseITMSQuery1, dfNoiseITMSQuery2, dfNoiseITMSQuery1Weighted

def postProcessing(dfNoise, configDict, genType):
    
    if genType == 'spatio-temporal':
        #postprocessing ITMSQuery1
        globalMaxValue = configDict['globalMaxValue']
        globalMinValue = configDict['globalMinValue']
        dfFinalITMSQuery1 = dfNoise
        dfFinalITMSQuery1['queryNoisyOutput'].clip(globalMinValue, globalMaxValue, inplace = True)
        dfFinalITMSQuery1.drop(['queryOutput'], axis = 1, inplace = True)
        
        '''
        #postprocessing ITMS Query 2
        dfFinalITMSQuery2 = dfNoiseITMSQuery2
        dfFinalITMSQuery2['query2NoisyOutput'].clip(0, np.inf, inplace = True)
        dfFinalITMSQuery2.drop(['query2Output'], axis = 1, inplace = True)
        '''
        return dfFinalITMSQuery1
    
    elif genType == 'categorical':
        dfFinal = dfNoise
        dfFinal['roundedNoisyCount']=dfFinal['noisyCount'].round()
        dfFinal['roundedNoisyCount'].clip(0, np.inf, inplace = True)
        dfFinal.drop(['noisyCount'], axis = 1, inplace = True)
        return dfFinal

def signalToNoise(signal,noise,configDict):
    # SNR Threshold
    snrUpperLimit = configDict['snrUpperLimit']
    snrLowerLimit = configDict['snrLowerLimit']
    # snr defined as signal mean over std of noise
    #signalPower/noisePower
    snr = (np.mean(signal*signal))/(np.var(noise))
    if snr < snrLowerLimit :
        print("Your Signal to Noise Ratio of " + str(round(snr,3)) + " is below the bound.")
    elif snr > snrUpperLimit:
        print("Your Signal to Noise Ratio of " + str(round(snr,3)) + " is above the bound.")
    else:
        print("Your Signal to Noise Ratio of " + str(round(snr,3)) + " is within the bounds.")
    return snr

def cumulativeEpsilon(configDict):

    privacyLossBudgetQuery1 = configDict['privacyLossBudgetEpsQuery'][0]
    privacyLossBudgetQuery2 = configDict['privacyLossBudgetEpsQuery'][1]
    cumulativeEpsilon = privacyLossBudgetQuery1 + privacyLossBudgetQuery2
    print('Your Cumulative Epsilon for the displayed queries is: ' + str(cumulativeEpsilon))
    return cumulativeEpsilon

def outputFile(dfFinal, dataframeName):
    # dataframeName = input('What would you like to name the output dataframe?')
    dfFinal.to_csv('../pipelineOutput/' + dataframeName + '.csv')
    return
