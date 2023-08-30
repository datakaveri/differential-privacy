#implementation of k-anonymity on a dataset
import pandas as pd
import numpy as np
import time
start_time=time.time()
end_time=0
def isKAnonymized(df,partition,sensitive_column, k,l):
    unique_values_count = df.loc[partition, sensitive_column].nunique()
    if (len(partition) < k) or unique_values_count<l:
        return False
    return True
#function for calculating the spans of each column in a partition 
def get_spans(df, partition,categorical, scale=None ):
    spans = {}
    for column in df.columns:
        if column in categorical:
            span = len(df[column][partition].unique())
        else:
            span = df[column][partition].max()-df[column][partition].min()
        if scale is not None:
            span = span/scale[column]
        spans[column] = span
    return spans
'''split function takes a dataframe, partition and column and 
returns two partitions that split the given partition such that
 all rows with values of the column column below the median are in one partition 
 and all rows with values above or equal to the median are in the other.'''

#need  to change the split function such that it creates half half bins every time
def split(df, partition, categorical,column):
    dfp = df[column][partition]
    if column in categorical:
        values = dfp.unique()
        lv = set(values[:len(values)//2])
        rv = set(values[len(values)//2:])
        return dfp.index[dfp.isin(lv)], dfp.index[dfp.isin(rv)]
    else:        
        median = dfp.median()
        dfl = dfp.index[dfp < median]
        dfr = dfp.index[dfp >= median]
        return (dfl, dfr)
def partition_dataset(df, feature_columns,categorical,numerical,scale,sensitive_column,k,l_div):
    #assigning the index of the whole dataframe
    working_partitions = [df.index]

    '''partition_flags = {}

    for partition in working_partitions:
        partition_flags[partition] = True'''

    spans = get_spans(df[categorical],df.index,categorical, scale)
    for column, span in sorted(spans.items(), key=lambda x:-x[1]):
        l=len(working_partitions)
        unique_categories = df[column].unique()
        while l>0:
            partition = working_partitions.pop(0)
            category_partitions = {category: [] for category in unique_categories}
            for idx in partition:
                category = df.at[idx, column]
                category_partitions[category].append(idx)
            partition_lists = [partition for partition in category_partitions.values()]
            flag=0
            for list in partition_lists:
                if isKAnonymized(df,list,sensitive_column,k,l_div)==False:
                    flag=1
                    break
            if flag==0:
                for partition in partition_lists:
                    working_partitions.append(partition)
            else:
                working_partitions.append(partition)
            l=l-1
    prev_l=len(working_partitions)
    while True:
        spans = get_spans(df[numerical], partition,categorical, scale)
        for column, span in sorted(spans.items(), key=lambda x:-x[1]):
            print("column:",column)
            l=len(working_partitions)
            while l>0:
                partition = working_partitions.pop(0)
                lp, rp = split(df, partition,categorical, column)
                if not isKAnonymized(df,lp,sensitive_column,k,l_div) or not isKAnonymized(df,rp,sensitive_column,k,l_div):
                    working_partitions.append(partition)
                else:
                    working_partitions.extend((lp, rp))
                l=l-1
        if prev_l==len(working_partitions):
            break
        else:
            prev_l=len(working_partitions) 
                    

    return working_partitions

def get_anonymize_dataset(df,finished_partitions,feature_columns,numerical):
    list_df=[]
    for partition in finished_partitions:
        rows = df.iloc[partition].copy()
        for column in numerical:
            max_value = rows[column].max()
            min_value = rows[column].min()
            rows[column]="["+str(min_value)+" - "+str(max_value)+"]"
        new_df = pd.DataFrame(columns=df.columns)
        new_df = pd.concat([new_df, rows],ignore_index=True)
        final_df = new_df.filter(feature_columns, axis=1)
        list_df.append(final_df)
    return list_df
def anonymize(filename):
    global end_time
    print("\nFor implementing only K-Anonymity choose l value to be either 0 or 1\n")
    while True:
        try:
            k = int(input("Enter the desired value of k:"))
            break
        except ValueError:
            print("Error: Please enter a valid integer for k.")
    while True:
        try:
            l = int(input("Enter the desired value of l:"))
            if l > k:
                print("Error: l cannot be greater than k. Please try again.")
            else:
                break 
        except ValueError:
            print("Error: Please enter a valid integer for l.")
    df = pd.read_csv(filename)
    feature_columns=["Name","Age","Pincode","Gender","Marital Status","Temperature","Medical Condition"]
    sensitive_column="Medical Condition"
    scale=None
    categorical=["Gender", "Marital Status"]
    numerical=["Age","Pincode","Temperature"]
    finished_partitions = partition_dataset(df, feature_columns,categorical,numerical,scale,sensitive_column, k, l)
    print("Number of partitions created:",len(finished_partitions))
    list_dfn = get_anonymize_dataset(df, finished_partitions, feature_columns, numerical)
    end_time=time.time()
    '''merged_df = pd.concat(list_dfn, ignore_index=True)
    sorted_df = merged_df.sort_values(by='Name')'''
    with open("k_l_anonymity.txt", "w") as output_file:
        for df in list_dfn:
            output_file.write(df.to_string())
            output_file.write("\n###############\n")
    if l==0 or l==1:
        with open("k_anonymity.txt", "w") as output_file:
            for df in list_dfn:
                output_file.write(df.to_string())
                output_file.write("\n###############\n")
        #for l=0 groups having same senistive attribute values
        with open("k_drawback.txt", "w") as output_file:
            for df in list_dfn:
                if df['Medical Condition'].nunique()==1:
                    output_file.write(df.to_string())
                    output_file.write("\n###############\n")
        with open("median_problem.txt", "w") as output_file:
            for df in list_dfn:
                if len(df)>=2*k:
                    output_file.write(df.to_string())
                    output_file.write("\n###############\n")

anonymize('kdata.csv')
print("Run time:",end_time-start_time)