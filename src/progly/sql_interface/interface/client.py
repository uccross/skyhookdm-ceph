from parser import handleQuery 

def changeSettings(input): 
    if input == 'options':
        return True
    else: 
        return False

def main(): 
    print("Enter options as comma separated values belows. Press 'Enter' without input to use default options.")
    print("Options (must be in this order): [num_objs],[pool_name]") # TODO: --use-cls and --quiet 
    print("Example: 2, tpchdata")
    opts = input(">>> ")

    # Run until told otherwise.
    while(1):
        print("Enter a SQL query or SQL file (Multiple semi-colon separated queries can be accepted).")
        # print("Enter 'options' to change options. Enter 'file [file_1] ...' to use one or more SQL files.") # TODO: Formatting for changings options and using SQL files
        rawUserQuery = input(">>> ")

        if changeSettings(rawUserQuery):
            print("Enter new options: ")
            opts = input(">>> ")
            continue

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


if __name__ == "__main__":
    main()
