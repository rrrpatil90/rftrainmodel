import unittest
from RFTrainModel.rftrainmodel.rftrainmodel import input_df, target_df, training
#import sys
#from pandas.util.testing import assert_frame_equal

# inputFile = sys.argv[1]
# targetFile = sys.argv[2]

inputFile = "E:\DMLE Project\Madhu\data\HierarchyInputWithBFMResult_brandFamily.csv"
targetFile = "E:\DMLE Project\Madhu\data\\target_brandFamily.csv"

input_header = ['manualMappedLabel', 'inputLabel', 'bfmMappedLabel']
target_header = ['targetLevel']

class TestFunc():
    
    @staticmethod
    def inputdf(inputFile):
        input_data = input_df(inputFile)
        return input_data
    
    @staticmethod
    def targetdf(targetFile):
        target_data = target_df(targetFile)
        return target_data
    
    @staticmethod
    def training(inputLevelWithBFM,targetLevelName):
        traindata = training(inputLevelWithBFM, targetLevelName)
        return traindata

class TestUM(unittest.TestCase,TestFunc):
    
    def setUp(self):
        pass
    
    def test_input_df_columns(self):
        self.assertEqual(list((TestFunc.inputdf(inputFile)).columns), input_header)
        #self.assertEqual(list((input_df(inputFile)).columns), input_header)
    
    def test_target_df_columns(self):
        self.assertListEqual(list((TestFunc.targetdf(targetFile)).columns), target_header)
    
    def test_input_df_count(self):
        self.assertEquals( len((TestFunc.inputdf(inputFile)).index), 4079)
    
    def test_target_df_count(self):
        self.assertEquals( len((target_df(targetFile)).index), 600)

    def test_training(self):
        self.assertEquals(len(TestFunc.training(TestFunc.inputdf(inputFile), TestFunc.targetdf(targetFile)).columns), len((training(input_df(inputFile),target_df(targetFile))).columns))

if __name__ == '__main__':
    unittest.main()
