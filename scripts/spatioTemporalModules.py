# Modules used to run Differential-Privacy Pipeline
import pandas as pd
import numpy as np
import json
import jsonschema
from pandas import json_normalize
import matplotlib.pyplot as plt
import h3
import math
from math import exp
from itertools import chain

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
    return dataframe

def timeRange(dataframe):
    #calculating the number of days in the dataset
    print(dataframe)
    startDay = dataframe['Date'].min()
    endDay = dataframe['Date'].max()
    timeRange = 1 + (endDay - startDay).days
    return timeRange

def filtering(dataframe, configDict):
     #filtering average num of occurences per day per HAT
    dfFinalGrouped = dataframe
    date = dfFinalGrouped["Date"].unique()
    minEventOccurencesPerDay = int(configDict["minEventOccurences"])
    limit = len(date) * minEventOccurencesPerDay
    dfFiltered = dfFinalGrouped.groupby(['HAT', 'Date']).agg({'license_plate':'nunique'}).reset_index()
    # //TODO license_plate to be replaced with generic input from the config file
    dfFiltered = dfFiltered.groupby(['HAT']).agg({'license_plate':'sum'}).reset_index()
    dfFiltered.rename(columns={"license_plate": "license_plate_count"}, inplace=True)
    dfFiltered = dfFiltered[dfFiltered['license_plate_count'] >= limit]
    dfFiltered = dfFinalGrouped["HAT"].isin(dfFiltered["HAT"])
    dfFinalGrouped = dfFinalGrouped[dfFiltered]
    # dfFinalGrouped.to_csv('groupingTestMultiple.csv')
    print('Number of unique HATs left after filtering is: ' + str(dfFinalGrouped['HAT'].nunique()))
    print('########################################################################################')
    # print(dfFinalGrouped)
    return dfFinalGrouped

def sensitivityFrame(dataframe):
    # only needed for secure enclave implementation
    dfSensitivity = dataframe.groupby(['HAT', 'license_plate', 'Date']).agg({'count': ['count']})
    dfSensitivity.columns = dfSensitivity.columns.droplevel(0)
    dfSensitivity.reset_index(inplace = True)

    dfCount = dfSensitivity.groupby(['HAT']).agg(
                            max_count=('count', 'max'),
                            sum_count=('count', 'sum'))
    dfCount.reset_index(inplace = True)
    # print('dfCount', len(dfCount))

    return dfSensitivity, dfCount

def ITMSQuery1(dataframe):
    #average speed of buses passing through a HAT

    dfITMSQuery1 = dataframe
    #weighted mean
    dfITMSQuery1HATSum = dfITMSQuery1.groupby('HAT').agg({'sum':'sum'})
    dfITMSQuery1HATCount = dfITMSQuery1.groupby('HAT').agg({'count':'sum'})
    dfITMSQuery1 = dfITMSQuery1HATSum['sum']/dfITMSQuery1HATCount['count']
    dfITMSQuery1 = dfITMSQuery1.to_frame().reset_index()
    dfITMSQuery1.rename(columns = {0:'queryOutput'}, inplace = True)
    return dfITMSQuery1

# def ITMSQuery1a(dataframe, K, configDict):
    # print("REACHED Q1a")
    # print("Running optimized Query1")
    # hats = np.unique(dataframe['HAT'])
    # eps_prime = configDict["privacyLossBudgetEpsQuery"][0] / K
    # dfITMSQuery1a, signalQuery1a, noiseQuery1a, bVarianceQuery1a = [], [], [], []
    # for h in hats:
    #     df_hat = dataframe[dataframe['HAT'] == h]
    #     q, s, n, b = give_me_private_mean(df_hat, eps_prime)
    #     dfITMSQuery1a.append(q)
    #     signalQuery1a.append(s)
    #     noiseQuery1a.append(n)
    #     bVarianceQuery1a.append(b)
    # # noisytvals, signals, noises = ...
    # dfITMSQuery1a = pd.DataFrame(dfITMSQuery1a)
    # dfITMSQuery1a.rename(columns = {0:'queryNoisyOutput'}, inplace = True)
    # signalQuery1a = pd.DataFrame(signalQuery1a)
    # noiseQuery1a = pd.DataFrame(noiseQuery1a)
    # signalQuery1a.rename(columns = {0:'queryOutput'}, inplace = True)
    # noiseQuery1a = dfITMSQuery1a
    # return signalQuery1a, noiseQuery1a,bVarianceQuery1a

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
def license_plates_HAT(df,HAT,day):
    filtered_df = df[(df['HAT'] == HAT) & (df['Date'] == day)]
    license_plates_list = filtered_df['license_plate'].tolist()
    return license_plates_list
def assign_speed_to_idx(speed, my_dict):
    for key in my_dict:
        lower_bound, upper_bound = key
        if lower_bound <= speed <= upper_bound:
            return my_dict[key]
def clip_values(lst, c):
    sum=0
    #print("c value:",c)
    #c=float(c)
    #print("c type:",type(c))
    for i in lst:
        sum=sum+i
    for i in range(0,len(lst)):
        lst[i]=(c*lst[i])/max(c,sum)
    return lst
def find_key(bins,value):
    for key,val in bins.items():
        if val==value:
            return key

def flatten_list(nested_list):
    return [item for sublist in nested_list for item in (flatten_list(sublist) if isinstance(sublist, list) else [sublist])]
def change_list(speed):
    list=[]
    for val in speed:
        if val>0:
            list.append(val)
    return list

def Histo_query(df,min_speed,max_speed,epsilon):
    HAT_list=df['HAT'].unique().tolist()
    epsilon_hat=epsilon/len(HAT_list)

    # Define the bin edges including both min_speed and max_speed
    bin_edges = np.arange(min_speed, max_speed + 5, 5)
    bins={}
    d=max_speed//5
    j=0
    #Assigning an index for each bin
    for i in range(0,len(bin_edges)-1):
            key=(bin_edges[i],bin_edges[i+1])
            bins[key]=j
            j=j+1
    for hat in HAT_list:
        m_h=[]
        N_h=[0]*d
        #print("start length of N_h:",len(N_h))
        #exit(0)
        filtered_df=df[df['HAT'] == hat]
        DATE_list=filtered_df['Date']
        N={}
        for day in DATE_list:
            N[day]={}
            plate_list=license_plates_HAT(df,hat,day)
            for plate in plate_list:
                filtered_df = df[(df['license_plate'] == plate)]
                # print(filtered_df)
                speed=filtered_df['speed']
                # print(speed)
                speed = flatten_list(speed)
                speed=change_list(speed)
                m_h.append(len(speed))
                print(speed)
                for speed_val in speed:
                    if plate in N:
                        idx=assign_speed_to_idx(speed_val,bins)
                        N[day][plate][idx]=N[day][plate][idx]+1
                    else:
                        N[day][plate]=[0]*d 
                        idx=assign_speed_to_idx(speed_val,bins)
                        N[day][plate][idx]=N[day][plate][idx]+1
        n=0
        for val in m_h:
            n=n+val
        max_value = max(m_h)
        print("MAx value in m_h",max_value)
        quantile=1-(1/n)*math.ceil((2*d)/epsilon_hat)
        print("quantile", quantile)
        c=private_quantile(m_h,quantile,epsilon_hat/2,max_value)
        for key,value in N.items():
            for subkey,list in value.items():
                new_list=clip_values(list,c)
                N_h = [n + l_element for n, l_element in zip(N_h, new_list)]
                #N_h=N_h+clip_values(list,c)
                print("N_h is:",N_h)
                print("length of N_h is:",len(N_h))
        print("hat_list:",len(HAT_list))
        print("c",c)
        print("laplace:",(4*c)/epsilon_hat)
        print("epsilon_hat",epsilon_hat)
        random_list=np.random.laplace(0,(4*c)/epsilon_hat,d)
        N_h=N_h+random_list
        #N_h = [n + l_element for n, l_element in zip(N_h,random_list)]
        print("final list after adding random value:",N_h)
        #N_h=N_h+[np.random.laplace(0,(4*c)/epsilon_hat)]*d
        x_values=[]
        print("length bins dictionary:",len(bins))
        for i in range (0,d):
            key=find_key(bins,i)
            x_values.append(str(key[0])+' - '+str(key[1]))
        # Plot the bar graph
        plt.bar(x_values, N_h, width=0.8, align='center', alpha=0.7)
        plt.xlabel('X-axis Values')
        plt.ylabel('Counts')
        plt.title('Histogram Plot for HAT value'+hat)
        plt.grid(True)
        # Set x-axis labels
        plt.xticks(x_values, rotation=45)
        plt.savefig("../pipelineOutput/"+hat+"_histogram.png")
        exit(0)

def sensitivityComputeITMSQuery(configDict, timeRange, dfCount):
    maxValue = configDict['globalMaxValue']
    minValue = configDict['globalMinValue']
    # sensitivity for weighted query 1
    sensitivityITMSQuery1 = ((dfCount['max_count']*(maxValue - minValue))/(dfCount['sum_count']))

    # sensitivity for query 2
    # sensitivity is computed per day, number of violations per HAT can only change by 1, so max change per day is 1/no. of days
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

    # computing noise weighted query 1
    bITMSQuery1 = sensitivityITMSQuery1/epsPrimeQuery1
    bITMSQueryVariance1 = 2 * (bITMSQuery1 * bITMSQuery1)
    noiseITMSQuery1 = np.random.laplace(0, bITMSQuery1)
    
    # computing noise query 2
    bITMSQuery2 = sensitivityITMSQuery2/epsPrimeQuery2
    bITMSQueryVariance2 = 2 * (bITMSQuery2 * bITMSQuery2)
    bITMSQueryVariance2 = [bITMSQueryVariance2]
    noiseITMSQuery2 = np.random.laplace(0, bITMSQuery2, len(dfNoiseITMSQuery2))

    # adding noise to the true value
    dfNoiseITMSQuery1['queryNoisyOutput'] = dfNoiseITMSQuery1['queryOutput'] + noiseITMSQuery1
    dfNoiseITMSQuery2['queryNoisyOutput'] = dfNoiseITMSQuery2['queryOutput'] + noiseITMSQuery2
 
    return dfNoiseITMSQuery1, dfNoiseITMSQuery2, bITMSQueryVariance1, bITMSQueryVariance2
def private_quantile(data, quantile, epsilon, Lambda):
    Z=data
    k = len(Z)
    Z.sort()
    print("Lamda:-",Lambda)
    Z= [min(max(0, z), Lambda) for z in Z]
    Z=[0]+Z+[Lambda]
    y = [(Z[i+1] - Z[i]) * np.exp(-(epsilon/2) * abs(i  - quantile*k)) for i in range(k+1)]
    sum_y = sum(y)
    # print("sum_y", sum_y)
    # print("y", y)
    print("k", k)
    list_absfunc = []
    for i in range(k+1):
        list_absfunc.append(Z[i+1]-Z[i])
    print("Z[i+1]-Z[i]", list_absfunc)
    probabilities = [y_i / sum_y for y_i in y]
    # Sampling an index i based on the probabilities
    i = np.random.choice(range(k+1), p=probabilities)
    # Sampling a uniform draw from Zi+1 - Zi
    uniform_draw = np.random.uniform(Z[i], Z[i+1])
    print(quantile," quantile of the speed data is ",uniform_draw)
    return uniform_draw
def snrCompute(signal, bVariance):
    snr =[]
    if (len(bVariance) == 1):
        for i in range(0, len(signal)):
            snr.append((signal[i]*signal[i])/(bVariance[0]))
    else:
        for i in range (0, len(signal)):
            snr.append((signal[i]*signal[i])/(bVariance[i]))
    return snr

def maeCompute(signal, estimate):
    mae = np.mean(np.abs(signal - estimate))
    return mae