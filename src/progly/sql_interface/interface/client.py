import re
import sys
import signal
from parser import handleQuery 
from optparse import OptionParser

def changeOptions(opts):
    possibleOptions = [
        'num_objs',
        'pool',
        # 'cls' # TODO: Expose cls and quiet to users
        # 'quiet'
    ]
    newOpts = input("\nWhat options do you want to change? " + str(possibleOptions) + "\n>>> ")
    newOpts = re.split(', | ',newOpts)

    for item in newOpts:
        if item not in possibleOptions:
            print("Invalid option: " + item + "\nReturning to SQL Client...\n")
            break
        val = input("What value do you want to change [" + item + "] to?\n>>> ")
        opts[item] = val
    print(opts)
    return opts

def prettyPrintHelp():
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
        

def prettyPrintIntro():
    print('{:^100}'.format("Welcome to the Skyhook SQL Client Application")) 
    print('{:^100}'.format("---------------------------------------------"))
    print('{:^100}'.format("(Enter 'help' for a brief summary of supported commands)\n"))

def main():
    usage = "usage: python3 %prog [options]"
    optParser = OptionParser(usage)
    optParser.add_option("-c", "--use-cls", action="store_true", dest="cls", default=True, help="push execution onto storage servers using object classes")
    optParser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="see summary of query results only")
    optParser.add_option("-n", "--num-objs", default=2, help="number of storage objects.") 
    optParser.add_option("-p", "--pool", default="tpchdata", help="name of object pool")
    (options, args) = optParser.parse_args()

    prettyPrintIntro()

    print("Enter a SQL query (multiple semi-colon separated queries can be accepted).")
    # print("Enter 'options' to change options. Enter 'file [file_1] ...' to use one or more SQL files.") # TODO: Formatting for changings options and using SQL files
    
    # Run until told otherwise.
    while True:
        try: 
            rawUserQuery = input(">>> ")

            opts = vars(options)
            # TODO: Implement pythonic switch-case using dicts for checking input? 
            if rawUserQuery == 'options': # TODO: Expose option changing to user
                print("Current options: ", end=' ')
                print(opts)
                opts = changeOptions(opts)
                continue
            if rawUserQuery == 'help':
                prettyPrintHelp()
                continue
            if rawUserQuery == 'quit':
                break

            if rawUserQuery.split()[0] == 'file': 
                print(rawUserQuery)
                for file in rawUserQuery.split(' ', 1)[1].split():
                    print(rawUserQuery.split(' ', 1)[1])
                    print(file)
                    with open(file) as f: 
                        # Max 10MB. TODO: Implement lazy method with 'yield' to read file piece by piece for large files
                        size = 1024  
                        read_queries = f.read(size)
                    handleQuery(opts, read_queries)
                continue

            handleQuery(opts, rawUserQuery)
        except:
            continue

if __name__ == "__main__":
    main()
