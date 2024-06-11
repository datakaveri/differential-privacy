# import statements
import pandas as pd
import numpy as np
import h3

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
    dataframe["Date"] = pd.to_datetime(dataframe[temporalAttribute]).dt.date
    dataframe["Time"] = pd.to_datetime(dataframe[temporalAttribute]).dt.time
    time = dataframe["Time"]
    dataframe["Timeslot"] = time.apply(lambda x: x.hour)
    dataframe.drop(columns = temporalAttribute, inplace = True)
    return dataframe

# assigning HATs from H3index and timeslot
def HATcreation(dataframe):
    dataframe["HAT"] = ( dataframe["Timeslot"].astype(str) + " " + dataframe["h3index"])
    print('\nNumber of unique HATs created is: ' + str(dataframe['HAT'].nunique()))
    return dataframe

# Filtering time slots by start and end time from config file
def temporalEventFiltering(dataframe, configFile):
    configFile = configFile["temporal_generalize"]
    startTime = configFile["start_time"]
    endTime = configFile["end_time"]
    dataframe = dataframe[(dataframe["Timeslot"] >= startTime) & (dataframe["Timeslot"] <= endTime) ]
    print('Number of unique timeslots left after temporal event filtering is: ' + str(dataframe['Timeslot'].nunique()))
    print('########################################################################################')
    return dataframe

# //TODO: Are two kinds of filtering required?
# Filtering by average number of events per HAT per day
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
    print('Number of unique HATs left after spatio-temporal event filtering is: ' + str(dataframe['HAT'].nunique()))
    print('########################################################################################')
    return dataframe

# performing differential privacy
def spatioTemporalDifferentialPrivacy(dataframeAccumulate, configFile, timeRange):
    dpConfig = configFile["differential_privacy"]
    count = dataframeAccumulate["query_output"].sum()

    # appropriate sensitivity computation
    if dpConfig["dp_query"] == "mean":
        sensitivity = (dpConfig["global_max_value"] - dpConfig["global_min_value"])/(count)
    elif dpConfig["dp_query"] == "count":
        sensitivity = (1/timeRange)
    
    # noise generation
    # TODO:// Epsilon prime consideration
    b = sensitivity/dpConfig["dp_epsilon_step"]
    noise = np.random.laplace(0, b, len(dataframeAccumulate))
    print(len(noise))
    # noise addition
    privateAggregateDataframe = dataframeAccumulate.copy()
    privateAggregateDataframe["noisy_output"] = privateAggregateDataframe["query_output"] + noise
    # print(privateAggregateDataframe)
    return privateAggregateDataframe