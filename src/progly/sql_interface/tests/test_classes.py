import unittest
import os 
import sys
import time 

from interface.query import Query
from interface.skyhook import SkyhookRunner
from interface.parser import SQLParser

class TestBasicQueries(unittest.TestCase): 
    #### Projection Queries ####
    def test_a_projection_1(self):
        q = Query()
        
        pass
    
    def test_b_projection_3_quiet(self):
        pass
    
    def test_c_projection_4_no_quiet_no_cls(self):
        pass
    
    #### Selection Queries ####
    def test_d_selection_1(self): 
        pass

    def test_e_selection_3_quiet(self):
        pass
    
    def test_f_selection_4_no_quiet_no_cls(self):
        pass

    #### Skyhook Runner ####
    def test_g_skyhook_cmd(self):
        pass

    #### Parser ####
    def test_h_parse_query(self):
        pass

if __name__ == '__main__':
    unittest.main()
