from parser import handleQuery 
from optparse import OptionParser

def changeSettings(input): 
    if input == 'options':
        return True
    else: 
        return False

def prettyPrintHelp():
    print("This is a help message")

def prettyPrintIntro():
    print('{:^100}'.format("Welcome to the Skyhook SQL Client Application")) 
    print('{:^100}'.format("---------------------------------------------"))
    # print('{:>10}'.format("Here you can enter SQL queries such as: "))
    # print('{:^50}'.format("\t\tSELECT * FROM lineitem; SELECT orderkey, tax, comment FROM lineitem\n"))
    print('{:^100}'.format("(Enter 'help' for a brief summary of supported commands)"))
    # print('{:^50}'.format("To change options enter 'options'; otherwise, if you need help enter 'help'.\nTo quit enter 'quit'"))

def main():
    usage = "usage: python3 %prog [options]"
    optParser = OptionParser(usage)
    optParser.add_option("-c", "--use-cls", action="store_true", dest="cls", default=True, help="push execution onto storage servers using object classes")
    optParser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="see summary of query results only")
    optParser.add_option("-n", "--num-objs", default=2, help="number of storage objects.")
    optParser.add_option("-p", "--pool", default="tpchdata", help="name of object pool")

    (options, args) = optParser.parse_args()
    print(options)
    print(args)

    prettyPrintIntro()

    print("Enter a SQL query (Multiple semi-colon separated queries can be accepted).")
    # print("Enter 'options' to change options. Enter 'file [file_1] ...' to use one or more SQL files.") # TODO: Formatting for changings options and using SQL files
    
    # Run until told otherwise.
    while(1):
        rawUserQuery = input(">>> ")

        if changeSettings(rawUserQuery):
            print("Enter new options: ")
            opts = input(">>> ")
            continue

        if rawUserQuery == 'help':
            prettyPrintHelp()
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
                handleQuery(options, read_queries)
            continue

        handleQuery(opts, rawUserQuery)


if __name__ == "__main__":
    main()
