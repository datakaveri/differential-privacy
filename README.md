# Differential Privacy User Guide

## Differential Privacy at a Glance
Differential Privacy describes a promise made by a data holder or curator to a data subject: “You will not be affected, adversely or otherwise, by allowing your data to be used in any study or analysis, no matter what other studies, data sets, or information sources, are available.”

Mathematically, this is represented as:

**<div align="center"> ∀ D and D′ ∀ x: ℙ[M(D) = x] ≤ exp(ε) ⋅ ℙ[M(D′) = x] </div>**


The differential privacy guarantee offered by this statement is that for any two neighbouring datasets D and D’ that differ only by the data of a single user, any function or process M is considered ε-differential private iff for every possible output x, the probability of this output being observed never differs by more than exp(ε) between the scenario with and without that user’s data.

In practice, this privacy preserving mechanism is achieved through the addition of noise sampled from a probability distribution function to the original data query. In our case, this distribution is the Laplacian or double-exponential, a symmetric and unimodal distribution.
DP-Pipeline Use Case
This Differential Privacy pipeline currently offers the option to choose a privacy loss budget ε and compute the amount of noise to be added by using the formula:

**<div align="center"> ε = S/b </div>**

where,  
 ε is the privacy loss budget,  
 S is the Sensitivity,  
 b is the noise

The tool is currently limited to a few temporal and geospatial datasets and work is being done to expand the functionality to all types of datasets.
## Pipeline
This pipeline has the following structure:

![image](https://user-images.githubusercontent.com/100753500/212615935-21161bfa-dc93-4356-ae7b-9be4a976277b.png)

The individual modules have the following functionality:

### Pre-Processing
#### Deduplication
This function removes and duplicates found in the dataset based on the choice of columns from the config file. If there exists two or more data packets with identical data in both columns then one of those packets is treated as a duplicate.

#### Suppression
This function removes unnecessary columns from the data as determined and selected by the user in the suppressCols parameter in the configuration file.

#### Generalisation and Delocalisation
This function generalises specific information in the dataset that could be used to reveal identifying information of the users whose data is present in the dataset. This could apply to location, time, etc. 

### ε-Differential Privacy Computation
#### Query Aggregation
These functions serve to implement the type of query that the user wants to observe in the data. Currently a limited number of queries and datasets are supported.

#### Noise Addition
This function computes the global sensitivity of the dataset and the amount of noise to be added based on the privacy loss budget assigned to each query. This noise is added to the aggregated query output.

### Post-Processing
#### Rounding and Clipping 
This function rounds the query outputs to ensure integer values are returned and clips to the lower and upper bounds defined by the global sensitivity. These are not required steps in the posprocessing pipeline.

#### Signal to Noise Ratio Validation
This function computes the ratio of signal to noise using the formula 


**<div align="center"> SNR =  μ/σ </div>**

where,  
μ is the mean of the true value or the signal,  
σ is the standard deviation of the noise parameter b  

If the SNR is too high, it is recommended that the parameter privacy budget be adjusted to avoid potential data leakage from addition of a very small amount of noise.

The output of the pipeline is an ε-differential private output for the specific query requested and the chosen value of ε.

### Setting up the Workspace
Clone the differential-privacy repository using the following command:

```
git clone https://github.com/datakaveri/differential-privacy.git
```

This sets up a directory with the structure seen below:
```
differential-privacy
├── config/
│   │   ├── DPConfig.json
│   │   ├── DPSchema.json
├── data/
├── scripts/
│   │   ├── .gitkeep
│   │   ├── h3testing.py
│   │   └── modules.py
├── requirements.txt
└── README.md
```
### Workspace Components
#### config/
This folder stores the config and schema files. In the config file, you can set up the pipeline parameters, such as ε, locality factor, etc. These are further described in the setting parameters section of this document.
data/
This folder holds the data that is to be processed with the differential privacy pipeline.
#### scripts/
This folder contains the modules of the pipeline as well as the execution script. Running the execution script runs the data through the pipeline.

#### requirements.txt
This file contains the package dependencies for this tool. Installation instructions are below:
Ensure that pip is installed on your machine. Navigate to the scripts folder, and run the following command to install the package and library dependencies:

```
pip install -r requirements.txt
```

### Setting Parameters
The config file DPConfig.json requires the user to set the following parameters:
##### duplicateDetection
- Input type: array of 2 strings (column names)
--  This parameter takes two inputs, which are column names in the dataset. If there exists two or more data packets with identical data in both columns then one of those packets is treated as a duplicate and the entire row is removed for that data packet.
##### suppressCols
- Input type: array of strings (column names)
- This parameter takes an array of strings as input, with the strings being column names in the dataset. These columns are treated as not required for the choice of query and are dropped from the output data.
##### groupbyCol
- Input type: string
- This parameter takes a column name as string, and is used to aggregate the chosen column for query creation
##### trueValue
- Input type: string
- This parameter takes a column name as string, and this column is treated as the column used for query creation and for application of a noise distribution to achieve ε-Differential Privacy.
##### localityFactor
- Input type: number
- This parameter is added to 1 and is used to define a “locality factor” that determines how close to true the values of certain parameters are.
- Applies to: globalMaxValue, globalMinValue, K 
##### globalMaxValue
- Input type: integer
- This parameter sets the maximum possible value for the trueValue parameter. This is used to compute the global sensitivity for certain queries and for post-processing (clipping).
##### globalMinValue
- Input type: integer
- This parameter sets the minimum possible value for the trueValue parameter. This is used to compute the global sensitivity for certain queries and for post-processing (clipping).
##### privacyLossBudgetEpsQuery
- Input type: array of integers
- This parameter determines the privacy loss budget for the queries chosen. The length of the array is equal to the number of queries.
##### h3Resolution
- Input type: array of integers
- This parameter determines the resolution of the hexagons used to visualize spatial data using Kepler.gl.
##### startTime
- Input type: integer
- This parameter is used to select a starting time slot to create a window of data to be processed further.
##### endTime
- Input type: integer
- This parameter is used to select an ending time slot to create a window of data to be processed further.
##### minEventOccurences
- Input type: integer
- This parameter is used to set a threshold to filter out aggregate query results that fall below that threshold. 
##### trueValueThreshold
- Input type: integer
- This parameter is used to filter out rows where the trueValue column values are below a certain threshold.
##### mapeThreshold
- Input type: integer
- This parameter is used to determine the threshold for the mean absolute percentage error between the true value and the noisy output value.
##### snrThreshold
- Input type: integer
- This parameter is used to determine the threshold for the signal (mean of the true value) to noise ratio (standard deviation of the noisy value)
### Running the Tool
To run the tool, ensure that the parameters are all defined in the config file in the format defined by the schema. Once that is done, run the following command:
```
python3 h3testing.py
```
