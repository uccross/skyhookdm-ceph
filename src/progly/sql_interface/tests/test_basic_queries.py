import unittest
import os 
import sys
import time 

from interface.translator import handleQuery

class TestBasicQueries(unittest.TestCase): 
    def test_a_projection_1_def(self):
        defaultOpts = {'use-cls': True, 
                'quiet': False, 
                'num-objs': 2, 
                'pool': 'tpchdata'}
        query = "SELECT * \
                 FROM lineitem"
        res = handleQuery(defaultOpts, query)
        with open('/expected/test_a_expected.txt') as f:
            expected = f.read()
        self.assertEqual(expected, res[0])
        time.sleep(10)
    
    def test_b_projection_2_def(self):
        defaultOpts = {'use-cls': True, 
                'quiet': False, 
                'num-objs': 2, 
                'pool': 'tpchdata'}
        query = "SELECT orderkey,tax,linenumber,returnflag \
            FROM lineitem"
        res = handleQuery(defaultOpts, query)
        time.sleep(10)
    
    def test_c_projection_3_quiet(self):
        opts = {'use-cls': False, 
                'quiet': True, 
                'num-objs': 2, 
                'pool': 'tpchdata'}
        query = "SELECT linenumber,returnflag \
                 FROM lineitem"
        res = handleQuery(opts, query)
        time.sleep(10)
    
    def test_d_projection_4_no_quiet_no_cls(self):
        opts = {'use-cls': False, 
                    'quiet': False, 
                    'num-objs': 2, 
                    'pool': 'tpchdata'}
        query = "SELECT shipdate, commitdate \
                 FROM lineitem"
        res = handleQuery(opts, query)
        time.sleep(10)
    
    def test_e_selection_1_def(self): 
        opts = {'use-cls': True, 
                    'quiet': False, 
                    'num-objs': 2, 
                    'pool': 'tpchdata'}
        query = "SELECT orderkey \
                 FROM lineitem \
                 WHERE orderkey > 3"
        res = handleQuery(opts, query) 
        time.sleep(10)
    
    def test_f_selection_2_quiet(self):
        opts = {'use-cls': True, 
                    'quiet': True, 
                    'num-objs': 2, 
                    'pool': 'tpchdata'}
        query = "SELECT orderkey,tax,linenumber,returnflag \
                 FROM lineitem \
                 WHERE orderkey <> 2"
        res = handleQuery(opts, query)
        time.sleep(10)
    
    def test_g_selection_3_quiet(self):
        opts = {'use-cls': True,
                'quiet': True,
                'num-objs': 2,
                'pool': 'tpchdata'}
        query = "SELECT orderkey,tax,linenumber,returnflag \
                 FROM lineitem \
                 WHERE orderkey != 2"
        res = handleQuery(opts, query)
        time.sleep(10)

if __name__ == '__main__':
    unittest.main()
