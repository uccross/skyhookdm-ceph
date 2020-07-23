import unittest
import os 
import sys
import time 

from interface.query import Query
from interface.skyhook import SkyhookRunner
from interface.parser import SQLParser

def get_expected_value(path):
    '''
    A function that returns the expected text results from the specified
    file path. 
    '''
    raise NotImplementedError

class TestBasicQueries(unittest.TestCase): 
    '''
    A testing module for each interface provided by the SkyhookSQL client. 
    '''

    #### Options for test validation ####
    sk_runner = SkyhookRunner()
    options_default = {'cls'            : True,
                      'quiet'            : False,
                      'header'           : True,
                      'pool'             : 'tpchdata',
                      'num-objs'         : '2',
                      'oid-prefix'       : '\"public\"',
                      'path_to_run_query': sk_runner.default_path}
    options_quiet = {'cls'              : True,
                     'quiet'            : True,
                     'header'           : True,
                     'pool'             : 'tpchdata',
                     'num-objs'         : '2',
                     'oid-prefix'       : '\"public\"',
                     'path_to_run_query': sk_runner.default_path}

    options_no_cls = {'cls'             : False,
                     'quiet'            : False,
                     'header'           : True,
                     'pool'             : 'tpchdata',
                     'num-objs'         : '2',
                     'oid-prefix'       : '\"public\"',
                     'path_to_run_query': sk_runner.default_path}

    #### Projection Queries ####
    def test_a_projection_1(self):
        # Init Query Object
        q = Query()

        # Verify default options
        self.assertEqual(q.options, options_default)
    
    def test_b_projection_2_quiet(self):
        # Init Query
        q = Query() 

        q.set_option('quiet', True)

        # Verify options use `quiet` setting
        self.assertEqual(q.options, options_quiet)
    
    def test_c_projection_3_no_cls(self):
        q = Query()
        
        q.set_option('cls', False)

        # Verify options do not use `cls` setting
        self.assertEqual(q.options, options_no_cls)
    
    #### Selection Queries ####
    def test_d_selection_1(self): 
        # Init Query Object
        q = Query()

        # Verify default options
        self.assertEqual(q.options, options_default)

    def test_e_selection_2_quiet(self):
        # Init Query
        q = Query() 

        q.set_option('quiet', True)

        # Verify options use `quiet` setting
        self.assertEqual(q.options, options_quiet)
    
    def test_f_selection_3_no_cls(self):
        q = Query()
        
        q.set_option('cls', False)

        # Verify options do not use `cls` setting
        self.assertEqual(q.options, options_no_cls)

    #### Skyhook Runner ####
    def test_g_skyhook_cmd(self):
        runner = SkyhookRunner()

    #### Parser ####
    def test_h_parse_query(self):
        parser = SQLParser()

if __name__ == '__main__':
    unittest.main()
