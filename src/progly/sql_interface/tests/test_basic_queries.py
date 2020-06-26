import unittest
import os 
from os import environ
import sys

# TODO: Setup venv for running sql client to get tests working 
from interface.translator import handleQuery

class TestBasicQueries(unittest.TestCase): 
    defaultOpts = {'use-cls': True, 
                   'quiet': False, 
                   'num-objs': 2, 
                   'pool': 'tpchdata'}

    def test_a_projection_1(self):
        query = "SELECT * \
                 FROM lineitem"
        handleQuery(defaultOpts, query)

    def test_b_projection_2(self):
        query = "SELECT orderkey,tax,linenumber,returnflag \
            FROM lineitem"
        handleQuery(defaultOpts, query)

    def test_c_projection_3(self):
        query = "SELECT linenumber,returnflag \
                 FROM lineitem"
        handleQuery(defaultOpts, query)

    def test_d_projection_4(self):
        query = "SELECT shipdate, commitdate \
                 FROM lineitem"
        handleQuery(defaultOpts, query)

    def test_e_selection_1(self): 
        query = "SELECT orderkey \
                 FROM lineitem \
                 WHERE orderkey > 3"
        handleQuery(defaultOpts, query) 

    def test_f_selection_2(self):
        query = "SELECT orderkey,tax,comment,lienumber,returnflag \
                 FROM lineitem \
                 WHERE orderkey <> 2"
        handleQuery(defaultOpts, query)

        query = "SELECT orderkey,tax,comment,lienumber,returnflag \
                 FROM lineitem \
                 WHERE orderkey != 2"
        handleQuery(defaultOpts, query)

if __name__ == '__main__':
    unittest.main()
