from .parser import handle_query
from .utils import print_help_msg, print_intro_msg
from .utils import ArgparseBuilder, PredefinedCommands

def main():
    optParser = ArgparseBuilder() 
    (options, args) = optParser.parse_args()

    print_intro_msg()

    print("Enter a SQL query (multiple semi-colon separated queries can be accepted).")
    
    # Run until told otherwise.
    while True:
        # try: 
        rawUserInput = input(">>> ")

        optsDict = vars(options)

        if rawUserInput == 'options': 
            print("Current options: ", end=' ')
            print(optsDict)
            optsDict = optParser.change_options(optsDict)
            continue

        if rawUserInput == 'help':
            print_help_msg()
            continue

        if rawUserInput == 'quit':
            break

        predefined = PredefinedCommands()
        # TODO: Check for case sensitivity 
        if 'describe table' in rawUserInput:
            table_name = rawUserInput.split(' ')[-1]
            show_table_schema = predefined.describe_table(table_name)
            print("Executing: " + show_table_schema['describe'])
            result = handle_query(optsDict, show_table_schema)

        if rawUserInput.split()[0] == 'file': 
            for file in rawUserInput.split(' ', 1)[1].split():
                with open(file) as f: 
                    # Max 10MB. TODO: Implement lazy method with 'yield' to read file piece by piece for large files
                    size = 1024  
                    read_queries = f.read(size)
                result = handle_query(optsDict, read_queries)
            continue

        result = handle_query(optsDict, rawUserInput)
        # except:
        #     continue

if __name__ == "__main__":
    main()
