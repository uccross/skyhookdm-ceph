import argparse
from optparse import OptionParser
import re
import sys

# ------------------------------
# Utility methods
def print_help_msg():
    print( 
'''
Skyhook SQL Client Application\nOptions:\n \t-n [--num-objs]\n \t-p [--pool]" 

Currently supported syntax:
\t Projections (EXAMPLE: SELECT orderkey FROM lineitem)
\t Selections  (EXAMPLE: SELECT orderkey FROM lineitem WHERE orderkey<3;)
\t SHOW schema (EXAMPLE: DESCRIBE TABLE lineitem)

To show this message enter:     'help'
To quit this application enter: 'quit' 
'''
        )


def print_intro_msg():
    print('{:^100}'.format("Welcome to the Skyhook SQL Client Application")) 
    print('{:^100}'.format("---------------------------------------------"))
    print('{:^100}'.format("(Enter 'help' for a brief summary of supported commands)\n"))

# ------------------------------
# Utility classes
class ArgparseBuilder():
    def __init__(self): #, arg_parser, **kwargs):
        # super(ArgparseBuilder, self).__init__(**kwargs)
        
        # self._arg_parser = arg_parser
        self.usage = "usage: python3 %prog [options]"
        self.optParser = OptionParser(self.usage)
        self.optParser.add_option("-c", "--use-cls", action="store_false", dest="use-cls",
            default=True, help="push execution onto storage servers using object classes")
        self.optParser.add_option("-q", "--quiet", action="store_true", dest="quiet",
            default=False, help="see summary of query results only")
        self.optParser.add_option("-n", "--num-objs", default=2, help="number of storage objects.",
            dest='num-objs') 
        self.optParser.add_option("-p", "--pool", default="tpchdata", help="name of object pool", 
            dest='pool')

    def parse_args(self):
        return self.optParser.parse_args()

    def change_options(self, optsDict):
        possibleOptions = [
            'num-objs',
            'pool',
            'use-cls', # TODO: Need to store use-cls and quiet as bool, not user input str 
            'quiet'
        ]
        try:
            newOptsDict = input("\nWhat options do you want to change? " + str(possibleOptions) + "\n>>> ")
            newOptsDict = re.split(', | ',newOptsDict)
        except KeyboardInterrupt as e:
            sys.exit(0)

        for item in newOptsDict:
            if item not in possibleOptions:
                print("Invalid option: " + item + "\nReturning to SQL Client...\n")
                break
            val = input("What value do you want to change [" + item + "] to?\n>>> ")
            optsDict[item] = val
        print(optsDict)
        return optsDict

class PredefinedCommands():
    def __init__(self):
        self.Predefined = None

    def describe_table(self, table_name):
        return {'describe':"bin/run-query --num-objs 2 --pool tpchdata --oid-prefix \"public\" --table-name \"{0}\" --header --limit 0".format(table_name)}