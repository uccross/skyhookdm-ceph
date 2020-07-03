from .parser import handle_query
from .utils import print_help_msg, print_intro_msg, ArgparseBuilder

def main():
    optParser = ArgparseBuilder() 
    (options, args) = optParser.parse_args()

    print_intro_msg()

    print("Enter a SQL query (multiple semi-colon separated queries can be accepted).")
    
    # Run until told otherwise.
    while True:
        try: 
            rawUserInput = input(">>> ")

            optsDict = vars(options)
            # TODO: Implement pythonic switch-case using dicts for checking input? 
            if rawUserInput == 'options': # TODO: Expose option changing to user
                print("Current options: ", end=' ')
                print(optsDict)
                optsDict = optParser.change_options(optsDict)
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
                result = handle_query(optsDict, showTableSchema)
                #print("hi")
            if rawUserInput.split()[0] == 'file': 
                for file in rawUserInput.split(' ', 1)[1].split():
                    with open(file) as f: 
                        # Max 10MB. TODO: Implement lazy method with 'yield' to read file piece by piece for large files
                        size = 1024  
                        read_queries = f.read(size)
                    result = handle_query(optsDict, read_queries)
                continue

            result = handle_query(optsDict, rawUserInput)
        except:
            continue

if __name__ == "__main__":
    main()
