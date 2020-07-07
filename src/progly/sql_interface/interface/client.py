from .utils import print_help_msg, print_intro_msg
from .utils import ArgparseBuilder, PredefinedCommands
from .query import Query

def main():
    arg_obj = ArgparseBuilder() 
    
    print_intro_msg()

    print("Enter a SQL query (multiple semi-colon separated queries can be accepted).")
    
    queryobj = Query()

    # Run until told otherwise.
    while True:
        # try: 
        raw_input = input(">>> ")

        if raw_input == 'options': 
            print("Current options: ", end=' ')
            print(arg_obj.args)
            arg_obj.args = arg_obj.change_options(arg_obj.args)
            continue

        if raw_input == 'help':
            print_help_msg()
            continue

        if raw_input == 'quit':
            break

        predefined = PredefinedCommands()
        # TODO: Check for case sensitivity 
        if 'describe table' in raw_input:
            table_name = raw_input.split(' ')[-1]
            show_table_schema = predefined.describe_table(table_name)
            print("Executing: " + show_table_schema['describe'])
            result = queryobj.handle_query(arg_obj.args, show_table_schema)

        if raw_input.split()[0] == 'file': 
            for file in raw_input.split(' ', 1)[1].split():
                with open(file) as f: 
                    # Max 10MB. TODO: Implement lazy method with 'yield' to read file piece by piece for large files
                    size = 1024  
                    read_queries = f.read(size)
                result = queryobj.handle_query(arg_obj.args, read_queries)
            continue

        result = queryobj.handle_query(arg_obj.args, raw_input)
        # except:
        #     continue

if __name__ == "__main__":
    main()
