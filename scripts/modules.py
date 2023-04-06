# Modules used to run Differential-Privacy Pipeline

import pandas as pd
import numpy as np
import json
import jsonschema
from pandas.io.json import json_normalize
import matplotlib.pyplot as plt
import h3
import itertools 
import matplotlib.pyplot as plt

def categorize(dataframe, configFile,genType):
    if genType == "spatio-temporal":
        dataframe = spatioTemporalGeneralization(dataframe, configFile)
    elif genType == "numeric":
        dataframe = numericGeneralization(dataframe)
    elif genType == "categorical":
        dataframe = categoricGeneralization(dataframe, configFile)
    return dataframe

def categoricGeneralization(dataframe, configFile):
    
    dataframe = dataframe.dropna(subset=[configFile['ID']])    
    
    # Split the individual values into categories
    listToSplit = configFile['splitCols']
    dataframe[listToSplit] = dataframe[listToSplit].apply(lambda x: x.str.split().str[-1])   
        
    return dataframe
    
def histogramQuery1(dataframe, configFile):   
    
    # set the value you want to filter on
    filter_value = configFile['CycleToKeep']
    
    # create a boolean index based on the condition that the value in the column is equal to the filter_value
    bool_index = dataframe['cycle'] == filter_value
    
    # filter the rows based on the boolean index
    dataframe = dataframe[bool_index]
    
    grouped = dataframe.groupby(configFile['queryPer'])

    # create a dictionary of dataframes where each key corresponds to a unique value in the column
    dfs = {}
    for name, group in grouped:
        dfs[name] = group
    
    
    histogramDfs = {}
    
    
    #print('size of data before hist' ,len(df))
    listToGrouped = configFile['groupByCols']
    
    
    for name, DF in dfs.items():
        
        #newDataframe = df.groupby(list_to_grouped).size().reset_index(name='Count')   
        crosstab = pd.crosstab(index=configFile['ID'], columns=[DF[col] for col in listToGrouped])
        crosstab = crosstab.reindex()
        
        # get all possible combinations of the columns
        all_combinations = itertools.product(*[DF[col].dropna().unique() for col in listToGrouped])
        
        # create an empty dictionary to store the counts
        counts = {}
        
        # iterate over all combinations and get the count from the cross-tabulation table
        for comb in all_combinations:
            #print(comb)
            if comb in crosstab.columns:
                count = crosstab.loc[:, tuple(comb)].values[0]
            else:
                count = 0
            counts[comb] = count
        
        # convert the dictionary to a DataFrame
        result = pd.DataFrame.from_dict(counts, orient='index', columns=['Count'])
        
        # reset the index and rename the columns
        result = result.reset_index()
        result[listToGrouped] = result['index'].apply(lambda x: pd.Series(x))    
        result = result.drop('index', axis=1)
        first_col = result.pop(result.columns[0])
        result = result.assign(Count=first_col)
        result = result.sort_values(by=listToGrouped)

        #print('size of data after hist' ,result['Count'].sum())
        histogramDfs[name] = result 
    
    return histogramDfs
    

def histogramGroup(dataframe, configFile):
    
    # set the value you want to filter on
    filter_value = configFile['CycleToKeep']
    
    # create a boolean index based on the condition that the value in the column is equal to the filter_value
    bool_index = dataframe['cycle'] == filter_value
    
    # filter the rows based on the boolean index
    dataframe = dataframe[bool_index]
    
    dataframe = dataframe.dropna()        
    # Split the individual values into categories
    listToSplit = configFile['splitCols']
    
    for col in listToSplit:
        new_cols = dataframe[col].str.split(' ',n=1, expand=True)
        new_cols[0] = pd.to_numeric(new_cols[0])
        new_cols = new_cols.rename(columns={0: col + '_num', 1: col + '_str'})
        dataframe = pd.concat([dataframe, new_cols], axis=1)
        dataframe.drop(col, axis=1, inplace=True)
        
    
    
    return dataframe

def histogramQuery2(dataframe, configFile):
    listToGrouped = configFile['groupByCols']
    val1 = 'N_str'
    val2 = 'N_num'
    
    newdf = pd.DataFrame()
    
    for colName in listToGrouped:
        dfColName = colName + '_num'
        
        if len(newdf)==0:
            newdf = dataframe.groupby(['village', 'district']).agg(                                
                                dfColName=(dfColName,'mean')
                               ).reset_index()
            newdf = newdf.rename(columns={'dfColName': dfColName+' mean'})
            
        else:
            print(dfColName)
            tempdf = dataframe.groupby(['village', 'district']).agg(                                
                                dfColName=(dfColName,'mean')
                               ).reset_index()
            print(tempdf)
            tempdf = tempdf.rename(columns={'dfColName': dfColName+' mean'})
            
            newdf = pd.merge(newdf, tempdf)
            
            
    dfGrouped = dataframe.groupby(['village', 'district']).agg(                                
                                mean=('N_num','mean')
                               ).reset_index()
    
    dfGrouped2 = dataframe.groupby([val1]).agg(
                                count=(val2,'count'),
                                max=(val2,'max'),
                                min=(val2,'min')).reset_index()
    
    return newdf, dfGrouped2

def noiseComputeHistogramQuery(dataframeDict, configFile):
    
    noisyDataframeDict = {}
    
    for name, dataframe in dataframeDict.items():
        bins = len(dataframe)
        epsilon = configFile['PrivacyLossBudget']
        if bins == 1 :
            sensitivity = 1
        else:
            sensitivity = 2    
        b = sensitivity/epsilon
        noisyHistogram = np.random.laplace(0, b, bins)
        dataframe['Noise']=noisyHistogram
        dataframe['noisyCount']=dataframe['Count']+noisyHistogram
        noisyDataframeDict[name] = dataframe
      
    return noisyDataframeDict
    
def printHistogram(df, name):
    
    # create figure and axis objects
    fig, ax = plt.subplots(dpi=800)
    
    # plot original data as bars
    ax.bar(df.index, df['Count'], alpha=0.5)
    
    # calculate deviation of noisy data from original data
    deviation = df['roundedNoisyCount'] 
    
    # plot deviation as dotted line
    ax.plot(df.index, deviation, 'x',markersize = 1, color='red')
    
    # set axis labels and title
    ax.set_xlabel('Index')
    ax.set_ylabel('Count')
    ax.set_title('Original Count vs Rounded Noisy Count')
    
    # show plot
    plt.show()
    
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

def readFile(configFileName):#reading config
    
    
    #reading config
    configFile = '../config/' + configFileName
    with open(configFile, "r") as cfile:
        configDict = json.load(cfile)
    
    #reading datafile
    dataFileName = '../data/' + configDict['dataFile']
    #with open(dataFileName, "r") as dfile:
    #  dataDict = json.load(dfile)
    
    #loading data
    #dataframe = pd.json_normalize(dataDict)
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
    else:
        dfGrouped = dfThreshold.groupby(['HAT']).agg(
                                count=(groupByCol,'count'))
        print('Warning: Only the count query is available for non-numeric choice of groupByCol')

    return dfGrouped

def ITMSQuery1(dataframe):
    #average speed per HAT
    dfITMSQuery1 = dataframe

    #getting average of average speeds
    dfITMSQuery1 = dfITMSQuery1.groupby('HAT').agg({'mean':'mean'}).reset_index()
    dfITMSQuery1.rename(columns = {'mean':'queryOutput'}, inplace = True)
    return dfITMSQuery1

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

    #since 'n' is the denominator in sensitivity, max change in sensitivity is from min value of 'n'
    N = dataframe['N'].min()
    return N

def KCompute(dataframe):
    #finding 'K', the maximum number of HATs a bus passes through per day
    dfK = dataframe.groupby(['Date','license_plate']).agg({'HAT':'nunique'}).reset_index()
    K = dfK['HAT'].max()
    return K

def ITMSSensitivityCompute(configDict, timeRange, N):
    maxValue = configDict['globalMaxValue']
    minValue = configDict['globalMinValue']

    # sensitivity for query 1
    sensitivityITMSQuery1 = (maxValue - minValue)/N   
    
    # sensitivity for query 2
    sensitivityITMSQuery2 = 1/timeRange

    return sensitivityITMSQuery1, sensitivityITMSQuery2

def noiseComputeITMSQuery(dfITMSQuery1, dfITMSQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K):
    dfNoiseITMSQuery1 = dfITMSQuery1
    dfNoiseITMSQuery2 = dfITMSQuery2

    # epsilon
    privacyLossBudgetEpsITMSQuery1 = configDict['privacyLossBudgetEpsQuery'][0]
    privacyLossBudgetEpsITMSQuery2 = configDict['privacyLossBudgetEpsQuery'][1]

    # computing epsilon prime
    epsPrimeQuery1 = privacyLossBudgetEpsITMSQuery1/K
    epsPrimeQuery2 = privacyLossBudgetEpsITMSQuery2/K

    # computing noise query 1
    bITMSQuery1 = sensitivityITMSQuery1/epsPrimeQuery1
    noiseITMSQuery1 = np.random.laplace(0, bITMSQuery1, len(dfNoiseITMSQuery1))

    # computing noise query 2
    bITMSQuery2 = sensitivityITMSQuery2/epsPrimeQuery2
    noiseITMSQuery2 = np.random.laplace(0, bITMSQuery2, len(dfNoiseITMSQuery2))

    # adding noise to the true value
    dfNoiseITMSQuery1['queryNoisyOutput'] = dfNoiseITMSQuery1['queryOutput'] + noiseITMSQuery1
    dfNoiseITMSQuery2['queryNoisyOutput'] = dfNoiseITMSQuery2['queryOutput'] + noiseITMSQuery2

    return dfNoiseITMSQuery1, dfNoiseITMSQuery2

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
