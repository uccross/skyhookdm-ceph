import argparse
import re
import sys

# ------------------------------
# Utility methods
def print_help_msg():
    print( 
'''
Skyhook SQL Client Application\n

To show this message enter:     'help'
To quit this application enter: 'quit' 

Options:\n \t -n [--num-objs]\n \t -p [--pool]\n \t -q [--quiet]\n \t -c [--use-cls] 

Input modes:
\t execute:    \e Enter a query that is immediately executed and stored in query object. (DEFAULT MODE)
\t\t usage: \e [<sql statement>, ...]

\t input:      \i Enter a query that is stored in query object.
\t\t usage: \i [<sql statement>, ...]

\t file:       \\f Enter SQL files. 
\t\t usage: \\f [<sql file>, ...]

\t options:    \o Enter new options to store in query object. 
\t\t usage: \o [<option>, ...]

\t manipulate: \m Enter an environment that enables manipulation of query object(s). 
\t\t usage: \m [<query_object>, ...]

Supported syntax:
\t Projections (EXAMPLE: SELECT orderkey FROM lineitem)
\t Selections  (EXAMPLE: SELECT orderkey FROM lineitem WHERE orderkey<3;)
\t Show Schema (EXAMPLE: DESCRIBE TABLE lineitem)

'''
        )


def print_intro_msg():
    print('{:^100}'.format("Welcome to the Skyhook SQL Client Application")) 
    print('{:^100}'.format("---------------------------------------------"))
    print('{:^100}'.format("(Enter 'help' for a brief summary of supported commands)\n"))
    print("Enter a SQL query (multiple semi-colon separated queries can be accepted).")

def prompt(curr_mode):
    modes = {'\e': 'execute',
            '\i': 'input',
            '\\f': 'files',
            '\o': 'options',
            '\q': 'query',
            'quit': 'quit',
            'help': 'help'}    
    i = input("({0}): ".format(modes[curr_mode]))
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
        self.opts = vars(self.arg_parser.parse_args())
        
    def change_options(self, arg_obj):
        print("In chane_opts argparse")
        optsDict = vars(arg_obj)['opts']
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

#     def set_options(self, argparse_obj):
#         '''
#         Set options from client via ArgparseBuilder object 
#         TODO: Handle --use-cls 'False' case. 
#         TODO: Is verbose more readable? (ifel conditions for each option)
#         '''
#         self.arg_obj = argparse_obj
#         for opt in self.options:
#             if self.arg_obj.opts[opt]:
#                 self.options[opt] = self.arg_obj.opts[opt]
        
#         self.command_template = "bin/run-query --num-objs " + str(self.options['num-objs']) + " \
# --pool " + str(self.options["pool"]) + " --oid-prefix \"public\" "

#         if self.options["use-cls"]:
#             self.command_template += "--use-cls "

#         if self.options["quiet"]:
#             self.command_template += ("--quiet ")

#         return

class PredefinedCommands():
    def __init__(self):
        self.Predefined = None

    def describe_table(self, table_name):
        return {'describe': "bin/run-query --num-objs 2 --pool tpchdata --oid-prefix \"public\" --table-name \"{0}\" --header --limit 0;".format(table_name)}

    # '''
    # Handle predefined Skyhook commands before SQL query
    # '''
    # # DESCRIBE TABLE T
    # if isinstance(raw_input, dict):
    #     if 'describe' in raw_input.keys():
    #         results = self.sk_runner.run_predefined(raw_input['describe'])
    #         return results