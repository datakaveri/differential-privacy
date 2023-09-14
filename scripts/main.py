import SECategoricalPipeline as SEC
import SESpatioTemporalPipeline as SES
import SEGenAggPipeline  as SEG
import preProcessing as premod
import json 

configFileName = '../config/anonymizationConfig.json'
with open(configFileName, "r") as cfile:
    configDict = json.load(cfile)

schemaFileName = 'anonymizationSchema.json'
premod.schemaValidator(schemaFileName, configFileName)

datasetType = configDict['genType']

validChoice = 0
while validChoice == 0: 
    if datasetType == 'spatio-temporal':       
        SES.main(configDict)
        validChoice = 1
    elif datasetType == 'categorical':
        SEC.main(configDict)
        validChoice = 1
    elif datasetType == 'genAgg':        
        SEG.main(configDict)
        validChoice = 1
    else:
        print("genType chosen in the configFile is invalid. Please check the configFile and try again.")
        break
