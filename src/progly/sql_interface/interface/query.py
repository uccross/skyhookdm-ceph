from .parser import SQLParser
from .skyhook import SkyhookRunner

class Query():
    def __init__(self):
        self.options = {'use-cls':  True,
                        'quiet':    False,
                        'pool':     'tpchdata',
                        'num-objs': 2}
        self.most_recent_query = {}
        self.queries = []
        self.dataframes = [] # TODO: Implement
        self.arg_obj = None
        self.command_template = None
    
    def set_options(self, argparse_obj):
        '''
        Sent options from client via dictionary of ArgparseBuilder object 
        TODO: Handle --use-cls 'False' case. 
        TODO: Is verbose more readable? (ifel conditions for each option)
        '''
        self.arg_obj = argparse_obj
        for opt in self.options:
            if self.arg_obj.opts[opt]:
                self.options[opt] = self.arg_obj.opts[opt]
        
        self.command_template = "bin/run-query --num-objs " + str(self.options['num-objs']) + " \
--pool " + str(self.options["pool"]) + " --oid-prefix \"public\" "

        if self.options["use-cls"]:
            self.command_template += "--use-cls "

        if self.options["quiet"]:
            self.command_template += ("--quiet ")

        return

    def change_options(self):
        # Wrapper for ArgparseBuilder() change_options
        print("in query change opts")
        self.options = self.arg_obj.change_options(self.arg_obj)
    
    def handle_query(self, raw_input):# options, raw_input):
        # TODO: Once a query is executed, remove from dictionary?
        # self.set_options(options)
        print(self.command_template)
        '''        
        sk_parser handles parsing and translation to Skyhook command
            * Parses SQL Query into respective selection, projeciton, etc parts
            * Hands these parts to sk_handler dictionary
            * Fills in template and sends to sk_handler runquery operations
        '''
        sk_parser = SQLParser(raw_input)

        '''
        sk_handler keeps track of parsed segments, option handling, query execution,
        and packaging dataframe objects as binaries if requested (e.g. arrow objects)
        '''
        sk_handler = SkyhookRunner()
        sk_handler.command_template = self.command_template

        '''
        Handle predefined Skyhook commands before SQL queries
        '''
        # DESCRIBE TABLE T
        if isinstance(raw_input, dict):
            if 'describe' in raw_input.keys():
                results = sk_handler.run_predefined(raw_input['describe'])
                return results

        queries = sk_parser.parse_query()
        print(queries)
        results = sk_handler.run_query(queries)

        return results

    def parse_query(self, statement):
        sk_parser = SQLParser(statement)
        res = sk_parser.parse_query()
        return res

    def set_query_selection(self):
        raise NotImplementedError

    def set_query_projection(self):
        raise NotImplementedError

    def set_query_table_name(self):
        raise NotImplementedError

    def set_query(self, statement):
        '''
        Parses query and appends to query kvs list
        '''
        parsed = self.parse_query(statement)
        self.queries.append(parsed)
        print("queries", end=' ')
        print(self.queries)

    def list_all_queries(self):
        '''
        Lists all queries 
        '''
        raise NotImplementedError

    def execute_cli_cmd(self, cmd):
        '''
        Executes a cli command via run-query that was generated
        by the query object. 
        '''
        raise NotImplementedError

    def generate_cli_cmd(self):
        '''
        Generates the command line command for run-query
        '''
        return

    def generate_dataframe(self):
        # Construct pyarrow dataframes
        raise NotImplementedError
