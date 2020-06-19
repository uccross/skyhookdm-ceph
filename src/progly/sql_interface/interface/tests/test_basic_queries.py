import unittest
import os 
from ..client import prettyPrintHelp, prettyPrintIntro

class TestBasicQueries(unittest.TestCase): 
    def test_a_projection(self):
        prettyPrintHelp()
        return 
    
    def test_b_selection(self): 
        prettyPrintIntro
        return 

if __name__ == '__main__':
    unittest.main()
