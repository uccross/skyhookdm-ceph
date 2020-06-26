import unittest
import os 
import sys

# TODO: Setup venv for running sql client to get tests working 
from interface.client import printHelpMsg, printIntroMsg

class TestBasicQueries(unittest.TestCase): 
    def test_a_projection(self):
        printHelpMsg()
        return 
    
    def test_b_selection(self): 
        printIntroMsg
        return 

if __name__ == '__main__':
    unittest.main()
