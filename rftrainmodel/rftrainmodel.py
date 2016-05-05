import pandas as pd
import numpy as np
from pyspark.sql import SQLContext
from pyspark import SparkContext
#from difflib import SequenceMatcher

sc = SparkContext(appName="MLRandomForestTrain")
sqlContext = SQLContext(sc)

# inputFile = "E:\DMLE Project\Madhu\data\HierarchyInputWithBFMResult_brandFamily.csv"
# targetFile = "E:\DMLE Project\Madhu\data\\target_brandFamily.csv"

# def jaccard_similarity(x, y):
#     intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
#     union_cardinality = len(set.union(*[set(x), set(y)]))
#     return intersection_cardinality / float(union_cardinality)

def input_df(inputFile):

    inputLevelWithBFMrdd = sc.textFile(inputFile)
    inputLevelWithBFMrdd = inputLevelWithBFMrdd.map(lambda line: line.split(","))
    header = inputLevelWithBFMrdd.first()
    inputLevelWithBFMrdd = inputLevelWithBFMrdd.filter(lambda line:line != header)
    sparkdf = inputLevelWithBFMrdd.toDF()
    df = sparkdf.toPandas()
    df.columns = header
    inputLevelWithBFM = df
    inputLevelWithBFM = pd.DataFrame(inputLevelWithBFM)
    return inputLevelWithBFM

def target_df(targetFile):

    targetLevelNamerdd = sc.textFile(targetFile) 
    targetLevelNamerdd = targetLevelNamerdd.map(lambda line: line.split(","))
    header = targetLevelNamerdd.first()
    targetLevelNamerdd = targetLevelNamerdd.filter(lambda line:line != header)
    sparkdf = targetLevelNamerdd.toDF()
    df = sparkdf.toPandas()
    df.columns = header
    targetLevelName = df
    targetLevelName = pd.DataFrame(targetLevelName)
    return targetLevelName

def training(inputLevelWithBFM,targetLevelName):

    targetLevelName = pd.DataFrame({'targetLevel': pd.unique(targetLevelName.targetLevel)})
    inputLevelWithBFM = inputLevelWithBFM[:50]
    np.random.seed([101])
    trainData = inputLevelWithBFM.sample(frac=.7, replace=False)
#    predDataAllOther = inputLevelWithBFM
    inputLabel_df = pd.DataFrame(trainData.loc[:, 'inputLabel'])
    inputLabel_df.loc[:, 'key'] = 0
    labelCompare = pd.merge(inputLabel_df, inputLabel_df, how='outer', on='key').drop(['key'], axis=1)
    labelCompare.columns = ['inputLabel', 'inputLabel_compare']
    labelCompare = labelCompare.drop(labelCompare[labelCompare['inputLabel'] == labelCompare['inputLabel_compare']].index)
    labelCompare.loc[:, 'inputNoSplChr'] = labelCompare.loc[:, 'inputLabel'].str.replace('[^0-9A-Za-z]', '')
    labelCompare.loc[:, 'inputCompareNoSplChr'] = labelCompare.loc[:, 'inputLabel_compare'].str.replace('[^0-9A-Za-z]', '')
    return labelCompare
'''
def label_compare(labelCompare):
    
    labelWithLCS = pd.DataFrame()
    labelCompareLength = len(labelCompare)
    
    ## Store the longest common substring, flag to determine whether a number is present and
    ## the length for each input label
    
    for i in range(0, (labelCompareLength)):
        a = pd.DataFrame(labelCompare.iloc[i, [2]])
        a = pd.DataFrame.to_string(a, header=False, index=False)[1:]
    
        b = pd.DataFrame(labelCompare.iloc[i, [3]])
        b = pd.DataFrame.to_string(b, header=False, index=False)[1:]
    
        c = list(labelCompare.iloc[i, [0]])
        s = SequenceMatcher(None, a, b)
        result = s.find_longest_match(0, len(a), 0, len(b))
        if result.size > 3:
            lcs = a[(result.a):(result.a + result.size)]
            labelWithLCS_tmp = pd.DataFrame({'longestCommonString': lcs, 'inputLabel': c})
            labelWithLCS = labelWithLCS.append(labelWithLCS_tmp, ignore_index=True)
    
    dummies = pd.get_dummies(labelWithLCS.loc[:, 'longestCommonString'])
    labelWithLCS_withdummy = pd.concat([labelWithLCS, dummies], axis=1)
    labelWithLCS_withdummy = labelWithLCS_withdummy.drop(['longestCommonString'], axis=1)
    labelWithLCSVariable = labelWithLCS_withdummy.groupby('inputLabel').max().reset_index()
    
    return labelWithLCSVariable

    ## Create dummy variables for all longest common substring categorical variables
    ## Remove 'longestCommonString' string from the dummy variable names
''' 
'''

if __name__ == '__main__':
     
    inputdf = input_df(inputFile)
    targetdf = target_df(targetFile)
    trainingdf = training(inputdf, targetdf)
    labelcomparedf = label_compare(trainingdf)
'''
