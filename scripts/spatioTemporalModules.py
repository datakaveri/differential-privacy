# import statements
from flask import config
import pandas as pd
import numpy as np
import h3
import logging

# select logging level
logging.basicConfig(level = logging.INFO)

# functions for generalization
##############################################################
# function to separate latitude/longitude and create H3index
def spatialGeneralization(dataframe, configFile):
    # separating latitude and longitude from location
    configFile = configFile["spatial_generalize"]
    lat_lon = dataframe[configFile["spatial_attribute"]]
    split_lat_lon = lat_lon.astype(str).str.strip('[]').str.split(', ')
    lon = split_lat_lon.apply(lambda x: x[0])
    lat = split_lat_lon.apply(lambda x: x[1])

    #assigning h3 index to the latitude and longitude coordinates in separate dataframe  
    length_of_dataframe = len(dataframe)
    h3index = [None] * length_of_dataframe
    resolution = configFile["h3_resolution"]
    for i in range(length_of_dataframe):
        h3index[i] = h3.geo_to_h3(lat=float(lat[i]), lng=float(lon[i]), resolution=resolution)
    dataframe["h3index"] = h3index
    return dataframe

# assigning date and time to separate dataframe and creating a timeslot column
def temporalGeneralization(dataframe, configFile):
    configFile = configFile["temporal_generalize"]
    temporalAttribute = configFile["temporal_attribute"]
    TSR = configFile["timeslot_resolution"]
    dataframe["Date"] = pd.to_datetime(dataframe[temporalAttribute]).dt.date
    dataframe["Time"] = pd.to_datetime(dataframe[temporalAttribute]).dt.time
    time = dataframe["Time"]
    dataframe["Timeslot"] = time.apply(lambda x: f'{x.hour}_{((x.minute)//TSR)*TSR}')
    dataframe.drop(columns = temporalAttribute, inplace = True)
    return dataframe

# assigning HATs from H3index and timeslot
def HATcreation(dataframe):
    dataframe["HAT"] = ( dataframe["Timeslot"].astype(str) + " " + dataframe["h3index"])
    logging.info('Number of unique HATs created is: ' + str(dataframe['HAT'].nunique()))
    return dataframe

# Filtering time slots by start and end time from config file
def temporalEventFiltering(dataframe, configFile):
    configFile = configFile["temporal_generalize"]
    temporalAttribute = configFile["temporal_attribute"]
    dataframe["Date"] = pd.to_datetime(dataframe[temporalAttribute]).dt.date
    dataframe["Time"] = pd.to_datetime(dataframe[temporalAttribute]).dt.time
    time = dataframe["Time"]
    dataframe["hour"] = time.apply(lambda x: x.hour)
    startTime = configFile["start_time"]
    endTime = configFile["end_time"]
    dataframe = dataframe[(dataframe["hour"] >= startTime) & (dataframe["hour"] <= endTime)]
    dataframe.reset_index()
    logging.info('Number of unique hours left after temporal event filtering is: ' + str(dataframe['hour'].nunique()))
    # logging.info('########################################################################################')
    return dataframe

# Filtering by average number of events(license plates) per HAT per day
def spatioTemporalEventFiltering(dataframe, configFile):
    # assigning required variables
    configFile = configFile["spatial_generalize"]
    date = dataframe["Date"].unique()
    minEventOccurencesPerDay = int(configFile["filter_event_occurences"])
    limit = len(date) * minEventOccurencesPerDay
    filterAttribute = configFile["filter_attribute"]
    filterBy = configFile["filter_attribute_by"]

    # filtering operations
    dfFiltered = dataframe.groupby([filterBy[0], filterBy[1]]).agg({filterAttribute:'nunique'}).reset_index()
    dfFiltered = dfFiltered.groupby([filterBy[0]]).agg({filterAttribute:'sum'}).reset_index()
    dfFiltered.rename(columns={filterAttribute: f"count_{filterAttribute}"}, inplace=True)
    dfFiltered = dfFiltered[dfFiltered[f"count_{filterAttribute}"] >= limit]
    dfFiltered = dataframe[filterBy[0]].isin(dfFiltered[filterBy[0]])
    dataframe = dataframe[dfFiltered]
    logging.info('Number of unique HATs left after spatio-temporal event filtering is: ' + str(dataframe['HAT'].nunique()))
    # logging.info('########################################################################################')
    return dataframe

# performing differential privacy
def spatioTemporalDifferentialPrivacy(dataframeAccumulate, configFile, timeRange):
    dpConfig = configFile["differential_privacy"]
    epsilon = dpConfig["dp_epsilon"]
    epsilon_step = dpConfig["dp_epsilon_step"]
    epsilon_vector = np.array([0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100])
    # epsilonVector = np.arange(1,30,epsilon_step).round(2)

    # appropriate sensitivity computation
    if dpConfig["dp_query"] == "mean":
        max_of_sum_of_counts_per_lp = dataframeAccumulate['max_of_sum_of_counts_per_lp']
        sum_of_counts = dataframeAccumulate['sum_of_counts']
        sensitivity = (max_of_sum_of_counts_per_lp*(dpConfig["global_max_value"] - dpConfig["global_min_value"]))/(sum_of_counts)
        bVector = np.zeros((len(sensitivity), len(epsilon_vector)))
        for i in range(len(epsilon_vector)):
            bVector[:, i] = sensitivity/epsilon_vector[i]
        bVector = pd.DataFrame(bVector, index=dataframeAccumulate.index, columns=epsilon_vector)
        bVector['HAT'] = dataframeAccumulate['HAT']
        
    elif dpConfig["dp_query"] == "count":
        sensitivity = 1
        bVector = np.zeros((1, len(epsilon_vector)))
        bVector = sensitivity/epsilon_vector
        bVector = pd.DataFrame(bVector, index=epsilon_vector)
    # noise generation
    b = sensitivity/epsilon

    # noise = np.random.laplace(0, bVector, (len(dataframeAccumulate), len(epsilon_vector)))
    noise = np.random.laplace(0, b, len(dataframeAccumulate))
    # print(len(noise))
    # noise addition
    privateAggregateDataframe = dataframeAccumulate.copy(deep=True)
    privateAggregateDataframe["noisy_output"] = privateAggregateDataframe["query_output"] + noise
    return privateAggregateDataframe, bVector