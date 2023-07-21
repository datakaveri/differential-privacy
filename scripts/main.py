import SECategoricalPipeline as SEC
import SESpatioTemporalPipeline as SES
import SEGenAggPipeline  as SEG
import json 

validChoice = 0
while validChoice == 0: 
    print("--------- IUDX DATA ANONYMIZATION PIPELINE ---------\n")
    print('''1. Spatio-Temporal Dataset (Surat ITMS)\n2. Categorical Dataset (Telangana Soil)\n3. genAgg Dataset (Vadodara Civic Complaints)\n''')
    datasetChoice = int(input("Enter a number to make your selection: "))
    if datasetChoice == 1:       
        SES.main()
        validChoice = 1
    elif datasetChoice == 2:
        SEC.main()
        validChoice = 1
    elif datasetChoice == 3:        
        SEG.main()
        validChoice = 1
    else:
        print("Choice Invalid, please enter an integer between 1 and 3 to make your choice. \\ ")
