from .parser import SQLParser
from .skyhook import SkyhookRunner

class Query():
    def __init__(self):
        '''
        A class that represents a mutable SQL query object.
        '''

        '''
        sk_runner keeps track of parsed segments, option handling, and query execution. 
        '''
        self.sk_runner = SkyhookRunner()
        
        '''        
        sql_parser translates raw SQL statements to dictionary representation
        '''
        self.sql_parser = SQLParser()

        self.raw_query = ''

        self.options = {'cls'      : True,
                        'quiet'    : False,
                        'pool'     : 'tpchdata',
                        'num-objs' : '2'}

        self.query = {'selection'  : '',
                      'projection' : '',
                      'table-name' : '',
                      'options'    : self.options}
        
        self.sk_cmd = ''
        self.results = None

    def sql(self, statement):
        '''
        Parses SQL statement and sets Query object parameters. 
        '''
        self.raw_query = statement
        parsed = self.parse_query(statement)
        self.set_table_name(parsed['table-name'])
        self.set_projection(parsed['projection'])
        self.set_selection(parsed['selection'])

    def run(self):
        '''
        A function that executes the Skyhook CLI command by calling the run-query
        binary.
        '''
        self.results = self.sk_runner.execute_sk_cmd(self.sk_cmd)

    def query_lifetime(self, raw_query):
        '''
        A function that performs a full query execution starting from a SQL statement, parsing it,
        setting the Query object's settings, generating a Skyhook command, and executing the command. 
        '''
        query = self.sk.parser.parse_query(raw_query)

        self.set_projection(query['projection'])
        
        self.set_selection(query['selection'])

        self.set_table_name(query['table-name'])

        self.generate_sk_cmd()

        self.run()

    def parse_query(self, statement):
        '''
        Parses a SQL statement and returns dictionary representation.
        '''
        res = self.sql_parser.parse_query(statement)
        return res

    def generate_sk_cmd(self):
        '''
        A function that generates the Skyhook CLI command representation of the 
        query object. 
        '''
        self.sk_cmd = self.sk_runner.create_sk_cmd(self.query)

    def generate_pyarrow_dataframe(self):
        '''
        An example of what may be done in the near future. 
        '''
        raise NotImplementedError


    def set_selection(self, input):
        '''
        Sets the selection parameter for a query.
        '''
        self.query['selection'] = input.split(', ')

    def set_projection(self, input):
        '''
        Sets the projection parameter for a query.
        '''
        self.query['projection'] = input

    def set_table_name(self, input):
        '''
        Sets the table name parameter for a query.
        '''
        self.query['table-name'] = input

    def set_option(self, option, value):
        '''
        Sets the option to be the given value.
        '''
        if option not in self.options.keys():
            print("Error: Not an option")
            return
        self.options[str(option)] = value 

    def show_query(self):
        '''
        A function that shows the current Query object. 
        '''
        return self.query

    def show_options(self):
        '''
        A function that shows the current options being used. 
        '''
        return self.query['options']

    def show_results(self):
        '''
        A function that shows the results of the previously ran query.  
        '''
        return self.results

    def show_sk_cmd(self):
        '''
        A function that shows the Skyhook CLI command representation of the
        query object. 
        '''
        return self.sk_cmd
