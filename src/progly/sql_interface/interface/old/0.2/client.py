from ..utils import print_help_msg, print_intro_msg, prompt
from ..utils import ArgparseBuilder, PredefinedCommands
from ..query import Query

class InputManager():
    def __init__(self):
        self.modes = {'execute': '\e',
                 'input': '\i',
                 'files': '\\f',
                 'options': '\o',
                 'query': '\q',
                 'quit': 'quit',
                 'help': 'help'}
        self.mru_mode = '\q' # Most recently used mode (defaults to \e)
        self.change_mode = False

    def run(self, query_obj):
        '''
        TODO: Run check once here, immediately going to default mode.
        Upon each return, update most recent mode if user inputted one
        and then branch to it
        '''
        while True:
            self.check(query_obj, self.mru_mode) 

    def check(self, query_obj, mode):
        '''
        check() manages control flow of application for users when`
        they want to switch input modes. 
        
        It is called at each iteration of each input mode before continuing. 
        '''

        '''
        TODO: More modes such as "list all queries in object" 
        '''
        if mode not in self.modes.values():
            print("Error: {} is not a mode.".format(mode))
            return 
        elif mode == self.modes['quit']:
            quit() 
        elif mode == self.modes['help']:
            print_help_msg() # TODO: Should prompt user again after this in same state 
            return
        elif mode == self.modes['execute']:
            self.enter_mode(query_obj, self.execute)
        elif mode == self.modes['input']:
            self.enter_mode(query_obj, self.input)
        elif mode == self.modes['files']:
            self.enter_mode(query_obj, self.files)
        elif mode == self.modes['options']:
            self.enter_mode(query_obj, self.options)
        elif mode == self.modes['query']:
            self.enter_mode(query_obj, self.query)
    
    def split_mode_and_statement(self, input):
        stub = ''
        split_input = input.split(' ', 1)
        if len(split_input) > 1:
            if split_input[0] not in self.modes.values():
                mode, statement = stub, split_input[0] + ' ' + split_input[1]
                return mode, statement
            else:
                mode, statement = split_input[0], split_input[1]
                return mode, statement

        elif len(split_input) == 1:
            if split_input[0] in self.modes.values():
                mode = split_input[0]
                return mode, stub
            else: 
                statement = split_input[0]
                return stub, statement

    def update_most_recent_mode(self, new_mode):
        if new_mode == self.modes['help']:
            return 
        self.mru_mode = new_mode

    def swap_change_mode(self):
        if self.change_mode:
            self.change_mode = False
        elif not self.change_mode:
            self.change_mode = True

    def enter_mode(self, query_obj, func):
        mode, statement = '', ''
        while not self.change_mode:
            user_input = prompt(self.mru_mode)
            
            mode, statement = self.split_mode_and_statement(user_input)
            
             # TODO: Prevents access to 'help' and 'quit'
            if mode == '' and statement == '':
                continue 
            
            if mode == self.mru_mode:
                func(query_obj, statement)
                continue 

            if mode is not '':
                print("Error: Switching modes is prohibited.")
                continue
                # self.update_most_recent_mode(mode)
                # self.swap_change_mode()
                # continue
                
            func(query_obj, statement)

        self.swap_change_mode()
        self.check(query_obj, mode)

    def execute(self, query_obj, input=None):
        '''
        Immediately execute and then store input in query_obj
        '''
        # TODO: Handle when user supplies mutliple statements?
        if input:
            query_obj.handle_query(input)
            query_obj.set_query(input)

    def input(self, query_obj, input=None):
        '''
        Store input in query_obj, do not execute it though
        '''
        # TODO: Handle when user supplies mutiples statements? 
        if input:
            query_obj.set_query(input)

    def files(self, query_obj, input=None):
        '''
        Parse file, splitting on sql statements, and store in query_obj
        '''
        if input.split()[0] == 'file': 
            for file in input.split(' ', 1)[1].split():
                with open(file) as f: 
                    # Max 10MB. TODO: Implement lazy method with 'yield' to read file piece by piece for large files
                    size = 1024  
                    read_queries = f.read(size)
                result = query_obj.handle_query(query_obj.arg_obj.opts, read_queries)
    
    def options(self, query_obj, input=None):
        '''
        Change query_obj options
        '''
        print("Current options: ", end=' ')
        print(query_obj.options)
        query_obj.change_options()

    def query(self, query_obj=None, input=None):
        '''
        Mode for full access to query_obj fields and methods
        '''
        raise NotImplementedError

def main():
    print_intro_msg()

    argparse_obj = ArgparseBuilder() 
    
    query_obj = Query()
    query_obj.set_default_options(argparse_obj)


    input_manager = InputManager()

    input_manager.query() 
    # input_manager.run(query_obj)

if __name__ == "__main__":
    main()

       # Run until told otherwise.
        # while True:
        #     # try:
        #     raw_input = prompt()

        #     if raw_input == 'options': 
                # print("Current options: ", end=' ')
                # print(query_obj.options)
                # query_obj.change_options()
                # continue

        #     if raw_input == 'help':
        #         print_help_msg()
        #         continue

        #     if raw_input == 'quit':
        #         break

        #     predefined = PredefinedCommands()
        #     # TODO: Check for case sensitivity 
        #     if 'describe table' in raw_input:
        #         table_name = raw_input.split(' ')[-1]
        #         show_table_schema = predefined.describe_table(table_name)
        #         print("Executing: " + show_table_schema['describe'])
        #         result = query_obj.handle_query(query_obj.arg_obj.opts, show_table_schema)

            # if raw_input.split()[0] == 'file': 
            #     for file in raw_input.split(' ', 1)[1].split():
            #         with open(file) as f: 
            #             # Max 10MB. TODO: Implement lazy method with 'yield' to read file piece by piece for large files
            #             size = 1024  
            #             read_queries = f.read(size)
            #         result = query_obj.handle_query(query_obj.arg_obj.opts, read_queries)
            #     continue

        #     result = query_obj.handle_query(query_obj.arg_obj.opts, raw_input)
        #     # except:
        #     #     continue
