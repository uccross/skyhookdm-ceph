from parser import handleQuery 

def changeSettings(input): 
    if input == 'options':
        return True
    else: 
        return False

def prettyPrintIntro():
    print('{:^100}'.format("Welcome to the Skyhook SQL Client Application")) 
    print('{:^100}'.format("---------------------------------------------"))
    print('{:>10}'.format("Here you can enter SQL queries such as: "))
    print('{:^50}'.format("\t\tSELECT * FROM lineitem; SELECT orderkey, tax, comment FROM lineitem\n"))
    print('{:^50}'.format("In order to change options enter 'options'; otherwise, if you need help enter 'help'\n"))

    #print("Enter options as comma separated values belows. Press 'Enter' without input to use default options.")
    #print("Options (must be in this order): [num_objs],[pool_name]") # TODO: --use-cls and --quiet 
    #print("Example: 2, tpchdata")

def main():
    prettyPrintIntro()
    opts = input(">>> ")
    print("Enter a SQL query (Multiple semi-colon separated queries can be accepted).")
    # print("Enter 'options' to change options. Enter 'file [file_1] ...' to use one or more SQL files.") # TODO: Formatting for changings options and using SQL files
    
    # Run until told otherwise.
    while(1):
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
