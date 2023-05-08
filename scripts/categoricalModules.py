import pandas as pd
import itertools 
import numpy as np
import matplotlib.pyplot as plt
from math import exp


def histogramQuery1(dataframe, configFile):       
    
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
    filter_value = configFile['cycleToKeep']
    
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
    grouped = dataframe.groupby(configFile['queryPer'])
    listToGrouped = configFile['groupByCols']
    allGroups = configFile['splitCols']
    
    cols = [x for x in allGroups if x not in listToGrouped]
 
    # drop columns
    dataframe = dataframe.drop(listToGrouped, axis=1)  

    # create a dictionary of dataframes where each key corresponds to a unique value in the column
    dfs = {}
    groupedModeDfs = {}
    truemodeDf = {}
    counts = pd.DataFrame()
    temp = pd.DataFrame()
    temp2 = pd.DataFrame()
    for name, group in grouped:
        dfs[name] = group
     
    for key in dfs:
        df = dfs[key]
        for col in cols:
        # Apply value_counts() on the current column
            vc = df[col].value_counts()
            # Convert the result to a DataFrame
            vc_df = pd.DataFrame({col: vc.index, 'count': vc.values})
            # Store the result under the original DataFrame
            if key not in groupedModeDfs.keys():
                groupedModeDfs[key] = {}
            groupedModeDfs[key][col] = vc_df
            

            
    for name, DF in dfs.items():
        truemodeDf[name] = DF[cols].agg(lambda x: x.mode()[0])
    truemodeDf = pd.DataFrame.from_dict(truemodeDf, orient='index')
    #print(counts)
    return groupedModeDfs,truemodeDf

def noiseComputeHistogramQuery2(dfs, configFile):
    noisy_mode = {}
    sensitivity = 1
    epsilon = configFile['PrivacyLossBudget'][1]
    for district in dfs:
        scores = []
        for col in dfs[district]:
            scores = np.array(dfs[district][col]['count'].values)
            max_score = np.max(scores)
            scores = scores - max_score
           
            # Calculate the maximum possible score (i.e., the mode)
            #max_score = max(scores)
                
            # Calculate the probabilities of selecting each item
            probabilities = np.array([exp(epsilon * score / (2 * sensitivity)) for score in scores])
            probabilities = probabilities / np.linalg.norm(probabilities, ord=1)
                
            # Select an item based on the probabilities
            selected_index = np.random.choice(len(dfs[district][col]), p=probabilities)
            selected_item = dfs[district][col].iloc[selected_index][0]
            #print(selected_item)
            if district not in noisy_mode.keys():
                noisy_mode[district] = {}
            noisy_mode[district][col] = selected_item
            #noisy_mode[district] = pd.DataFrame(noisy_mode[district])
            
    finalDF2 = pd.DataFrame.from_dict(noisy_mode, orient='index')

    return noisy_mode,finalDF2  
    
def reportNoisyMax(dfs):
    noisy_mode2 ={}
    sensitivity = 2
    epsilon = 0.001
    for district in dfs:
        scores = []
        for col in dfs[district]:
            scores = np.array(dfs[district][col]['count'].values)
            b = sensitivity/epsilon
            noise = np.random.laplace(0, b, len(scores))
            noisyScores = noise+scores
            #print(scores, noisyScores)
            max_idx = np.argmax(noisyScores)
            # Return the element corresponding to that index
            selected_item = dfs[district][col].iloc[max_idx][0]
            if district not in noisy_mode2.keys():
                noisy_mode2[district] = {}
            noisy_mode2[district][col] = selected_item
            #noisy_mode[district] = pd.DataFrame(noisy_mode[district])
            
    finalDF3 = pd.DataFrame.from_dict(noisy_mode2, orient='index')
    return noisy_mode2, finalDF3   

def test(dfNoiseQuery4, dfQuery4, configDict):
    for district in dfNoiseQuery4:
        print('##################### '+district)
        for col in dfNoiseQuery4[district]:
            print('---------------for col '+col+'\nPrinting for Exponential Mech')
            r = [noiseComputeHistogramQuery2(dfQuery4, configDict)[0][district][col] for i in range(200)]
            x = [reportNoisyMax(dfQuery4, configDict)[0][district][col] for i in range(200)]
            print(pd.Series(r).value_counts() )
            print('\n\nPrinting for RNM')
            print(pd.Series(x).value_counts() )

def noiseComputeHistogramQuery(dataframeDict, configFile):
    
    noisyDataframeDict = {}
    
    for name, dataframe in dataframeDict.items():
        bins = len(dataframe)
        epsilon = configFile['PrivacyLossBudget'][0]
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
    original_bars = ax.bar(df.index, df['Count'], alpha=0.5, label='Original Count')
    
    # calculate deviation of noisy data from original data
    deviation = df['roundedNoisyCount']
    
    # plot deviation as dotted line
    noisy_points = ax.plot(df.index, deviation, 'x', markersize=2, color='red', label='Rounded Noisy Count')
    
    # set axis labels and title
    ax.set_xlabel('Index')
    ax.set_ylabel('Count')
    ax.set_title('Original Count vs Rounded Noisy Count for ' + name)
    
    # add legend
    ax.legend()
    
    fig.savefig('../pipelineOutput/plots/'+name+'.png', dpi=800)

    # show plot
    plt.show()