import os

class SkyhookRunner:
    def __init__(self):
        '''
        A class that 
        '''
        self.default_path = "~/skyhookdm-ceph/build/ && /bin/run-query"

    def create_sk_cmd(self, query):
        '''
        A function that generates the Skyhook CLI command from a Query Object. 
        '''
        command_args = [
            '--num-objs'   , query['options']['num-objs'],
            '--pool'       , query['options']['pool'],
            '--oid-prefix' , query['options']['oid-prefix'],
            '--table-name' , "\"{}\"".format(query['table-name'])
        ]

        if query['options']['header']:
            command_args.append("--header")

        if query['options']['cls']:
            command_args.append("--use-cls")

        if query['options']['quiet']:
            command_args.append("--quiet")

        if query['projection']:
            command_args.append("--project \"{}\" ".format(query['projection']))

        if query['selection']:
            if len(query['selection']) == 3:
                command_args.append("--select \"{0},{1},{2}\" ".format(query['selection'][1],
                                                                        query['selection'][0],
                                                                        query['selection'][2]))

        skyhook_cmd = self.default_path
        for arg in command_args:
            skyhook_cmd = ' '.join([skyhook_cmd, str(arg)])

        return skyhook_cmd


    def execute_sk_cmd(self, command):
        '''
        A function that executes a Skyhook CLI command. 
        '''
        result = os.popen("cd " + command).read()
        return result

    def run_query(self, query): 
        '''
        A function that generates and executes a Skyhook CLI command starting from 
        a Query object. 
        '''
        cmd = create_sk_cmd(query)

        result = self.execute_command(cmd)
        
        return result

    def package_arrow_objects(self):
        '''
        A function to coordinate the joining of arrow objects return from a Skyhook CLI 
        command execution. 
        '''
        raise NotImplementedError
