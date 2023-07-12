import pandas as pd
import itertools 
import numpy as np
import matplotlib.pyplot as plt
from math import exp
import matplotlib.ticker as ticker
import postProcessing as postmod
import json

def categoricGeneralization(dataframe, configFile):
    
    dataframe = dataframe.dropna(subset=[configFile['ID']])    

    # Split the individual values into categories
    listToSplit = configFile['splitCols']
    dataframe[listToSplit] = dataframe[listToSplit].apply(lambda x: x.str.split().str[-1])   

    return dataframe

def histogramQuery1(dataframe, configFile):       
    
    grouped = dataframe.groupby(configFile['queryPer'])

    # create a dictionary of dataframes where each key corresponds to a unique value in the column
    dfs = {}
    for name, group in grouped:
        dfs[name] = group
    
    histogramDfs = {}
    
    #print('size of data before hist' ,len(df))
    listToGrouped = configFile['groupByCols']
    
    for listToGrouped in configFile['groupByPairs']:
        for name, DF in dfs.items():

            #newDataframe = DF.groupby(listToGrouped).size().reset_index(name='Count')   
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

            if name not in histogramDfs.keys():
                histogramDfs[name] = {}
            histogramDfs[name][listToGrouped[0]+' and '+listToGrouped[1]] = result.reset_index(drop = True)
            
    return histogramDfs

def noiseComputeHistogramQuery1(dataframeDict, configFile):    
    noisyDataframeDict = {}
    
    
    
    for name, df in dataframeDict.items():
        for pair, dataframe in df.items():
            bins = len(dataframe)
            epsilon = configFile['PrivacyLossBudget'][0]/len(configFile['groupByPairs'])  
            if bins == 1 :
                sensitivity = 1
            else:
                sensitivity = 2
            b = sensitivity/epsilon
            
            noisyHistogram = np.random.laplace(0, b, bins)  
            dataframe['Noise']=noisyHistogram
            dataframe['noisyCount']=dataframe['Count']+noisyHistogram
            #noisyDataframeDict[name] = dataframe
            if name not in noisyDataframeDict.keys():
                noisyDataframeDict[name] = {}
            noisyDataframeDict[name][pair] = dataframe.reset_index(drop = True)
    
    bVarianceQuery1 = 2*b*b
    
    return noisyDataframeDict, bVarianceQuery1
    
def histogramQuery2(dataframe, configFile):
    allCols = configFile['splitCols']
    computedCols = configFile['groupByPairs']
    grouped = dataframe.groupby(configFile['queryPer'])

    # create a dictionary of dataframes where each key corresponds to a unique value in the column
    dfs = {}
    groupedCountDfs = {}
    for name, group in grouped:
        dfs[name] = group
    
    for pair in computedCols:
        for element in pair:
            if element in allCols:
                allCols.remove(element)
                
    for key in dfs:
        df = dfs[key]
        for col in allCols:
        # Apply value_counts() on the current column
            vc = df[col].value_counts()
            # Convert the result to a DataFrame
            vc_df = pd.DataFrame({col: vc.index, 'Count': vc.values})
            # Store the result under the original DataFrame
            if key not in groupedCountDfs.keys():
                groupedCountDfs[key] = {}
            groupedCountDfs[key][col] = vc_df
            
    return groupedCountDfs, allCols

def noiseComputeHistogramQuery2(dfs, allCols, configFile):
    sensitivity = 2
# TODO   
    for df in dfs:
        epsilon = configFile['PrivacyLossBudget'][1]/len(allCols)
        b = sensitivity/epsilon
        bVarianceQuery2 = 2*b*b
        scores = []
        for col in dfs[df]:
            scores = np.array(dfs[df][col]['Count'].values)
            noise = np.random.laplace(0, b, len(scores))
            dfs[df][col]['Noise'] = noise
            noisyScores = noise+scores
            dfs[df][col]['noisyCount'] = noisyScores

    return dfs, bVarianceQuery2

def exponentialMechanismHistogramQuery2 (dfs, configFile):
    noisy_mode = {}
    sensitivity = 1
    epsilon = configFile['PrivacyLossBudget'][1]/len(dfs['Adilabad'])
    for district in dfs:
        scores = []
        for col in dfs[district]:
            scores = np.array(dfs[district][col]['Count'].values)
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

def printHistogram(df, name, pair, query):
    
    # create figure and axis objects
    fig, ax = plt.subplots(dpi=800)
    
    categories = list(df.iloc[:,0:2])
    if(query == 1):
        x_values = df[categories].apply(lambda x: '-'.join(x), axis=1)  # concatenate the values in the categorical columns
    else:
        x_values = df[pair]
    # plot original data as bars
    original_bars = ax.bar(x_values, df['Count'], alpha=0.5, label='Original Count')
    
    # calculate deviation of noisy data from original data
    deviation = df['roundedNoisyCount']
    
    # plot deviation 
    noisy_points = ax.plot(df.index, deviation, 'x', markersize=2, color='red', label='Rounded Noisy Count')
    
    # set axis labels and title
    ax.set_xlabel('Index')
    ax.set_ylabel('Count')
    if query == 1:
        ax.set_title('Count vs Rounded Noisy Count for ' + name + ' and mineral: '+pair)
    else:
        ax.set_title('Count vs Rounded Noisy Count for ' + name + ' and pair: '+pair)
    #ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    
    # add legend
    ax.legend()
   
   # set x-axis tick labels at 45 degree angle
    ax.set_xticks(range(len(x_values)))
    ax.set_xticklabels(x_values, rotation=90, fontsize=5) 
   
    fig.savefig('../pipelineOutput/plots/'+name+'_'+pair+'.png', dpi=800)

    plt.close()
    # show plot
    #plt.show()

def postProcessingQuery(noiseHistQuery, configDict, genType,):
    
    dfsFinal = {}
    
    for name, dfs in noiseHistQuery.items():
        for pair, dfsNoise in dfs.items():
            dfFinalHistQuery = postmod.postProcessing(dfsNoise, configDict, genType)
            
            if name not in dfsFinal.keys():
                dfsFinal[name] = {}
            dfsFinal[name][pair] = dfFinalHistQuery.reset_index(drop = True)
            
    return dfsFinal

def histogramAndOutputQuery(dfFinalQuery, configDict, genType, query):
    for name, dfs in dfFinalQuery.items():
        for pair, df in dfs.items():
            dfFinalQuery[name][pair] = df.drop(['Count','Noise'], axis = 1).reset_index(drop = True)
            #postmod.outputFile(dfFinalQuery[name][pair], 'dfNoisySoil_'+str(query)+name+'_'+pair)
    return dfFinalQuery
            
def snrQuery(noiseHistQuery, bVariance, configDict):
    for name, noisyDfs in noiseHistQuery.items():
        listOfSNR = []
        print('For '+name+ ': ', end='')
        for pair, finalDF in noisyDfs.items():
            
            #signal assignment
            signal = finalDF['Count'].reset_index(drop = True)
            
            #noise assignment
            #noiseQuery = finalDF['Noise'].reset_index(drop = True)
            
            #signal to noise computation
            snr = (np.mean(signal*signal))/(bVariance)
            listOfSNR.append(snr)
        
        snrAverage = np.mean(listOfSNR)
        snrVariance = np.var(listOfSNR)
        print('\nSNR Average : ' +  str(np.round(snrAverage,3)), end=' ')
        #postmod.signalToNoise(snrAverage, configDict)
        print('|| SNR Variance : ' + str(np.round(snrVariance, 3)))
        #postmod.signalToNoise(snrVariance, configDict)


    