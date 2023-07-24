import pandas as pd
import numpy as np
def genAggGeneralization(dataframe, configFile):
    #separating year and month and creating aggregate year_month column
    year = pd.to_datetime(dataframe[configFile["datetimeCol"]]).dt.year
    month = pd.to_datetime(dataframe[configFile["datetimeCol"]]).dt.month
    dataframe['yearMonth'] = year.astype(str) + '_' + month.astype(str)

    # create a ward + year month generalization
    # WAYM = Ward At a Year Month
    dataframe['WAYM'] = dataframe['yearMonth'] + ' ' + dataframe['wardID'].astype(str) 
    return dataframe
def resolutionCount(dataframe):
    WAYMD_dict = {}
    for WAYM, group in dataframe.groupby('WAYM'):
        df = group.pivot_table(index='department', columns='resolutionStatus', aggfunc='size', fill_value=0)
        WAYMD_dict[WAYM] = df
    return WAYMD_dict
def merge(dict1,dict2):
    for WAYM, df in dict2.items():
        if WAYM in dict1:
            df1=dict1[WAYM]
            merged_df = pd.merge(df1, df, how='outer', left_index=True, right_index=True)
            merged_df = df1.add(df, fill_value=0)
            merged_df.fillna(0, inplace=True)
            dict1[WAYM] = merged_df
        else:
            dict1[WAYM] = df
    return dict1