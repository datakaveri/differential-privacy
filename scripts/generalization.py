import pandas as pd
import numpy as np
import h3
import json


def categorize(dataframe, configFile,genType):
    if genType == "spatio-temporal":
        dataframe = spatioTemporalGeneralization(dataframe, configFile)
    elif genType == "numeric":
        dataframe = numericGeneralization(dataframe)
    # TODO: introduce new categories

    return dataframe

def spatioTemporalGeneralization(dataframe, configFile):

    # Spatial
    lat_lon = dataframe[configFile['col.location']]
    split_lat_lon = lat_lon.astype(str).str.strip('[]').str.split(', ')

    lon = split_lat_lon.apply(lambda x: x[0])
    lat = split_lat_lon.apply(lambda x: x[1])
    dfLen = len(dataframe)
    h3index = [None] * dfLen
    resolution = configFile["resolution"]
    
    for i in range(dfLen):
        h3index[i] = h3.geo_to_h3(lat=float(lat[i]), lng=float(lon[i]), resolution=resolution)

    dataframe["h3index"] = h3index

    # Temporal
    dataframe["Date"] = pd.to_datetime( dataframe[configFile["col.datetime"]]).dt.date
    dataframe["Time"] = pd.to_datetime( dataframe[configFile["col.datetime"]]).dt.time

    time = dataframe["Time"]
    dataframe["Timeslot"] = time.apply(lambda x: x.hour)

    # HAT
    dataframe["HAT"] = ( dataframe["Timeslot"].astype(str) + " " + dataframe["h3index"])

    # Filter date and time slots
    #start_date = pd.to_datetime(configFile["start_date"]).date()
    #end_date = pd.to_datetime(configFile["end_date"]).date()
    #start_time = configFile["start_time"].astype(int)
    #end_time = configFile["end_time"].astype(int)
    groupByColumn = configFile["col.groupyByCol"]

    #dataframe = dataframe[ (dataframe["Date"] >= start_date & dataframe["Date"] <= end_date) ]
    #dataframe = dataframe[ (dataframe["Timeslot"] >= start_time & dataframe["Timeslot"] <= end_time) ]

    # Selecting h3 indices where a min number of events occur in all timeslots of the day
    f1 = ( dataframe.groupby(["Timeslot", "Date", "h3index"]) .agg({groupByColumn: "nunique"}) .reset_index())
    f2 = f1.groupby(["Timeslot", "h3index"]).agg({groupByColumn: "sum"}).reset_index()

    date = dataframe["Date"].unique()
    min_event_occurances = int(configFile["minEventOccurances"])
    limit = len(date) * min_event_occurances
    f3 = f2[f2[groupByColumn] >= limit]
    f4 = f3.groupby("h3index").agg({"Timeslot": "count"}).reset_index()

    maxTimeslots = f4["Timeslot"].max()
    f5 = f4[f4["Timeslot"] == maxTimeslots]

    df = dataframe["h3index"].isin(f5["h3index"])
    dataframe = dataframe[df]
    print(dataframe)
    return dataframe

def numericGeneralization(dataframe, configFile):

    return dataframe

