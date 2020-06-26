import unittest
import os 
from os import environ
import sys

# TODO: Setup venv for running sql client to get tests working 
from interface.translator import handleQuery

class TestBasicQueries(unittest.TestCase): 
    def test_a_projection_1_def(self):
        defaultOpts = {'use-cls': True, 
                'quiet': False, 
                'num-objs': 2, 
                'pool': 'tpchdata'}
        query = "SELECT * \
                 FROM lineitem"
        handleQuery(defaultOpts, query)

    def test_b_projection_2_def(self):
        defaultOpts = {'use-cls': True, 
                'quiet': False, 
                'num-objs': 2, 
                'pool': 'tpchdata'}
        query = "SELECT orderkey,tax,linenumber,returnflag \
            FROM lineitem"
        handleQuery(defaultOpts, query)

    def test_c_projection_3_quiet(self):
        opts = {'use-cls': False, 
                'quiet': True, 
                'num-objs': 2, 
                'pool': 'tpchdata'}
        query = "SELECT linenumber,returnflag \
                 FROM lineitem"
        handleQuery(opts, query)

    def test_d_projection_4_no_quiet_no_cls(self):
        opts = {'use-cls': False, 
                    'quiet': False, 
                    'num-objs': 2, 
                    'pool': 'tpchdata'}
        query = "SELECT shipdate, commitdate \
                 FROM lineitem"
        handleQuery(opts, query)

    def test_e_selection_1_def(self): 
        opts = {'use-cls': True, 
                    'quiet': False, 
                    'num-objs': 2, 
                    'pool': 'tpchdata'}
        query = "SELECT orderkey \
                 FROM lineitem \
                 WHERE orderkey > 3"
        handleQuery(opts, query) 

    def test_f_selection_2_quiet(self):
        opts = {'use-cls': True, 
                    'quiet': True, 
                    'num-objs': 2, 
                    'pool': 'tpchdata'}
        query = "SELECT orderkey,tax,comment,lienumber,returnflag \
                 FROM lineitem \
                 WHERE orderkey <> 2"
        handleQuery(opts, query)

        query = "SELECT orderkey,tax,comment,lienumber,returnflag \
                 FROM lineitem \
                 WHERE orderkey != 2"
        handleQuery(opts, query)

if __name__ == '__main__':
    unittest.main()
