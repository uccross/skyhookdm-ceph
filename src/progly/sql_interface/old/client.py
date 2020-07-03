import re
import sys
from .translator import handle_query
from optparse import OptionParser

def change_options(optsDict):
    possibleOptions = [
        'num-objs',
        'pool',
        # 'cls' # TODO: Expose cls and quiet to users
        # 'quiet'
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

def print_help_msg():
    print( # TODO: Expose \t--use-cls\n \t--quiet\n 
'''
Skyhook SQL Client Application\nOptions:\n \t-n [--num-objs]\n \t-p [--pool]" 

Currently supported syntax:
\t Projections (EXAMPLE: SELECT orderkey FROM lineitem)
\t Selections  (EXAMPLE: SELECT orderkey FROM lineitem WHERE orderkey<3;)
\t SHOW schema (EXAMPLE: DESCRIBE TABLE lineitem)

To show this message enter:     'help'
To quit this application enter: 'quit' 
''')
        

def print_intro_msg():
    print('{:^100}'.format("Welcome to the Skyhook SQL Client Application")) 
    print('{:^100}'.format("---------------------------------------------"))
    print('{:^100}'.format("(Enter 'help' for a brief summary of supported commands)\n"))

def main():
    usage = "usage: python3 %prog [options]"
    optParser = OptionParser(usage)
    optParser.add_option("-c", "--use-cls", action="store_false", dest="use-cls",
        default=True, help="push execution onto storage servers using object classes")
    optParser.add_option("-q", "--quiet", action="store_true", dest="quiet",
        default=False, help="see summary of query results only")
    optParser.add_option("-n", "--num-objs", default=2, help="number of storage objects.",
        dest='num-objs') 
    optParser.add_option("-p", "--pool", default="tpchdata", help="name of object pool", 
        dest='pool')

    (options, args) = optParser.parse_args()

    print_intro_msg()

    print("Enter a SQL query (multiple semi-colon separated queries can be accepted).")
    # print("Enter 'options' to change options. Enter 'file [file_1] ...' to use one or more SQL files.") # TODO: Formatting for changings options and using SQL files
    
    # Run until told otherwise.
    while True:
        try: 
            rawUserInput = input(">>> ")

            optsDict = vars(options)
            # TODO: Implement pythonic switch-case using dicts for checking input? 
            if rawUserInput == 'options': # TODO: Expose option changing to user
                print("Current options: ", end=' ')
                print(optsDict)
                optsDict = change_options(optsDict)
                continue
            if rawUserInput == 'help':
                print_help_msg()
                continue
            if rawUserInput == 'quit':
                break
            if 'describe table' in rawUserInput:
                tableName = rawUserInput.split(' ')[-1]
                showTableSchema = "bin/run-query --num-objs 2 --pool tpchdata --oid-prefix \"public\" --table-name \"{0}\" --header --limit 0".format(tableName)
                print("Executing: {0}".format(showTableSchema))
                res = handle_query(optsDict, showTableSchema)
                #print("hi")
            if rawUserInput.split()[0] == 'file': 
                for file in rawUserInput.split(' ', 1)[1].split():
                    with open(file) as f: 
                        # Max 10MB. TODO: Implement lazy method with 'yield' to read file piece by piece for large files
                        size = 1024  
                        read_queries = f.read(size)
                    res = handle_query(optsDict, read_queries)
                continue

            res = handle_query(optsDict, rawUserInput)
        except:
            continue

if __name__ == "__main__":
    main()
