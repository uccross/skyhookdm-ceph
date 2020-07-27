import unittest
import os 
import sys
import time 

from interface.query import Query
from interface.skyhook import SkyhookRunner
from interface.parser import SQLParser

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


def get_expected_value(path):
    '''
    A function that returns the expected text results from the specified
    file path. 
    '''
    with open(path, 'r') as f:
        data = f.read()
        f.close()
    return data

class TestInterface(unittest.TestCase): 
    '''
    A testing module for each interface provided by the SkyhookSQL client. 
    '''

    #### Projection Queries ####
    def test_a_projection_1(self):
        # Init Query Object
        q = Query()

        # Verify default options
        self.assertEqual(q.options, options_default)

        q.sql("select * from lineitem")
        q.run()

        expected = get_expected_value(os.getcwd() + "/tests/expected/query/test_a_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)

    def test_b_projection_2_quiet(self):
        # Init Query
        q = Query() 

        q.set_option('quiet', True)

        # Verify options use `quiet` setting
        self.assertEqual(q.options, options_quiet)

        q.sql("select orderkey,discount,shipdate from lineitem")
        q.run()

        expected = get_expected_value(os.getcwd() + "/tests/expected/query/test_b_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)
    
    def test_c_projection_3_no_cls(self):
        q = Query()
        
        q.set_option('cls', False)

        # Verify options do not use `cls` setting
        self.assertEqual(q.options, options_no_cls)

        q.set_projection("orderkey,discount,shipdate")
        q.set_table_name("lineitem")

        expected_query = {'selection'  :'',
                          'projection' :'orderkey,discount,shipdate', 
                          'table-name' :'lineitem',
                          'options'    : options_no_cls}
        self.assertEqual(q.query, expected_query)

        q.run()

        expected = get_expected_value(os.getcwd() + "/tests/expected/query/test_c_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)
    
    #### Selection Queries ####
    def test_d_selection_1(self): 
        # Init Query Object
        q = Query()

        # Verify default options
        self.assertEqual(q.options, options_default)

        q.sql("select orderkey from lineitem where orderkey > 3")

        q.run()

        expected = get_expected_value(os.getcwd() + "/tests/expected/query/test_d_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)

    def test_e_selection_2_quiet(self):
        # Init Query
        q = Query() 

        q.set_option('quiet', True)

        # Verify options use `quiet` setting
        self.assertEqual(q.options, options_quiet)

        q.set_projection("orderkey,tax,commitdate")
        q.set_selection("lt, orderkey, 5")
        q.set_table_name("lineitem")

        expected_query = {'selection'  :['gt', 'orderkey', '3'],
                          'projection' :'tax,orderkey', 
                          'table-name' :'lineitem',
                          'options'    : options_quiet}

        self.assertNotEqual(q.query, expected_query)

        q.run()

        expected = get_expected_value(os.getcwd() + "/tests/expected/query/test_e_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)
    
    def test_f_selection_3_no_cls(self):
        q = Query()
        
        q.set_option('cls', False)

        # Verify options do not use `cls` setting
        self.assertEqual(q.options, options_no_cls)

        q.sql("SELECT * from lineitem where linenumber > 4")

        q.run()

        expected = get_expected_value(os.getcwd() + "/tests/expected/query/test_f_expected.txt")
        self.assertEqual(q.results, expected)

        time.sleep(5)

    #### Skyhook Runner ####
    def test_g_skyhook_cmd(self):
        runner = SkyhookRunner()

        q = Query()
        q.set_selection("gt, orderkey, 3")
        q.set_projection("shipdate,orderkey")
        q.set_table_name("lineitem")
        q.set_option("cls", False)
        q.set_option("header", False)
        q.set_option("quiet", True)
        q.set_option("num-objs", 10)
        q.set_option("pool", "name")

        cmd = runner.create_sk_cmd(q.query)

        expected = get_expected_value(os.getcwd() + "/tests/expected/skyhook/test_g_expected.txt")
        self.assertEqual(cmd, expected)

        time.sleep(5)

    #### Parser ####
    def test_h_parse_query(self):
        parser = SQLParser()

        parsed = parser.parse_query("select everything from thisTable where everything like 'nothing'")

        expected = get_expected_value(os.getcwd() + "/tests/expected/parser/test_h_expected.txt")
        self.assertEqual(str(parsed), expected) 

        time.sleep(5)

if __name__ == '__main__':
    unittest.main()
