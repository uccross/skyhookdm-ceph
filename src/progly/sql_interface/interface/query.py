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
    
    def set_options(self, opts):
        '''
        Sent options from client via dictionary of ArgparseBuilder object 
        TODO: Handle --use-cls 'False' case. 
        TODO: Is verbose more readable? (ifel conditions for each option)
        '''
        for opt in self.options:
            if opts[opt]:
                self.options[opt] = opts[opt]
        
        self.command_template = "bin/run-query --num-objs " + str(self.options['num-objs']) + " \
--pool " + str(self.options["pool"]) + " --oid-prefix \"public\" "

        if self.options["use-cls"]:
            self.command_template += "--use-cls "

        if self.options["quiet"]:
            self.command_template += ("--quiet ")

        return   

    def handle_query(self, options, raw_input):
        self.set_options(options)

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

        '''
        Handle predefined Skyhook commands before SQL queries
        '''
        # DESCRIBE TABLE T
        if isinstance(raw_input, dict):
            if 'describe' in raw_input.keys():
                results = sk_handler.run_predefined(raw_input['describe'])
                return results

        queries = sk_parser.parse_query()
        results = sk_handler.run_query(queries)

        return results

    def set_query(self):
        return 

    def generate_cli_cmd(self):
        return

    def generate_dataframe(self):
        # Construct pyarrow dataframes
        raise NotImplementedError
    


    