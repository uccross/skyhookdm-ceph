import argparse
import re
import sys

# ------------------------------
# Utility methods
def print_help_msg():
    print( 
'''
Skyhook SQL Client Application\n

Options:\n \t-n [--num-objs]\n \t-p [--pool]\n \t-q [--quiet]\n \t-c [--use-cls] 

Input modes:
\t execute:    \e Enter a query that is immediately executed and stored in query object. (DEFAULT)
\t input:      \i Enter a query that is stored in query object.
\t file:       \\f Enter SQL files. 
\t options:    \o Enter new options to store in query object. 
\t manipulate: \m Enter an environment that enables manipulation of query object. 

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
    print("Enter a SQL query (multiple semi-colon separated queries can be accepted).")

def prompt():
    i = input(">>> ")
    return i

# ------------------------------
# Utility classes
class ArgparseBuilder():
    def __init__(self): #, arg_parser, **kwargs):
        # super(ArgparseBuilder, self).__init__(**kwargs)
        
        self.arg_parser = argparse.ArgumentParser(usage='usage: python3 -m interface.client | ./startup.sh')
        self.arg_parser.add_argument('-c', 
                                '--use-cls', 
                                dest='use-cls', 
                                action='store_false', 
                                default=True, 
                                help='push execution onto storage servers using object classses')
        self.arg_parser.add_argument('-q', 
                                '--quiet', 
                                dest='quiet',
                                action='store_true', 
                                default=False, 
                                help='see summary of query results only')
        self.arg_parser.add_argument('-n', 
                                '--num-objs', 
                                dest='num-objs',
                                nargs=1,
                                default=2, 
                                type=int,
                                help='number of storage objects')
        self.arg_parser.add_argument('-p',
                                '--pool',
                                dest='pool',
                                nargs=1,
                                default='tpchdata',
                                type=str,
                                help='name of object pool')
        self.args = vars(self.arg_parser.parse_args())
        
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
        return {'describe': "bin/run-query --num-objs 2 --pool tpchdata --oid-prefix \"public\" --table-name \"{0}\" --header --limit 0;".format(table_name)}
