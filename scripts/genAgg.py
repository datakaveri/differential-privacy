import pandas as pd
# TODO: Generalization
# TODO: Filtering
# TODO: Aggregating
# TODO: query creation

'''
def predict_severity(text):
        severity_index = pipeline.predict([text])[0]
        print("text:- ",text)
        print(" Severity Index:", severity_index)
        return severity_index'''

'''trainDF = pd.read_excel('../data/traindata.xlsx')
comments = trainDF['Comment'].tolist()
severities = trainDF['Severity'].tolist()
# Combine the columns into a list of tuples
data = list(zip(comments, severities))

    # Print the extracted data

    # Prepare training data
X = [entry[0] for entry in data]
y = [entry[1] for entry in data]
# Create a pipeline for text classification
pipeline = Pipeline([
        ('vectorizer', TfidfVectorizer()),
        ('classifier', LogisticRegression(max_iter=100))
    ])
    # Train the model
pipeline.fit(X, y)'''



def Generalization(dataframe, configFile):
    #separating year and month and creating aggregate year_month column
    year = pd.to_datetime(dataframe[configFile["datetimeCol"]]).dt.year
    month = pd.to_datetime(dataframe[configFile["datetimeCol"]]).dt.month
    dataframe['yearMonth'] = year.astype(str) + '_' + month.astype(str)

    # create a ward + year month generalization
    # WAYM = Ward At a Year Month
    dataframe['WAYMD'] = dataframe['yearMonth'] + ' ' + dataframe['wardID'].astype(str) + ' ' +dataframe['department']
    WAYMDCounts=dataframe.pivot_table(index='WAYMD', columns='resolutionStatus', aggfunc='size', fill_value=0).reset_index()
    #filtering to enforce minimum number of records per WAYM
    '''eventThreshold = configFile['eventThreshold']
    dataframe = dataframe.groupby('WAYM').agg({'reportID': 'count'}).reset_index()
    dataframe = dataframe[dataframe['reportID'] >= eventThreshold] '''
    return WAYMDCounts