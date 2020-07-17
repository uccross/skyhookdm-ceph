import os

class SkyhookRunner:
    def __init__(self):
        self.command_list = []
        self.command_template = None
        self.prog = "cd ~/skyhookdm-ceph/build && "

    def package_arrow_objects(self):
        return

    def package_flatbuf_objects(self):
        return

    def run_predefined(self, cmd):
        print("Executing: " + self.prog + cmd)
        res = os.popen(self.prog + cmd).read()
        return res

    def create_template(self, options):        
        self.command_template = "bin/run-query --num-objs " + str(options['--num-objs']) + " \
--pool " + str(options["--pool"]) + " --oid-prefix \"public\" "

        if options["--use-cls"]:
            self.command_template += "--use-cls "

        if options["--quiet"]:
            self.command_template += ("--quiet ")

    def create_cli_cmd(self, query):
        cmd = ""
        self.create_template(query['options'])

        if query['table-name']:
            cmd = self.command_template + '--table-name "{0}" '.format(query['table-name'])

        if query['selection']:
            cmd += '--select "{0},{1},{2}" '.format(query['selection'][1],
                                                    query['selection'][0],
                                                    query['selection'][2])

        if query['projection']:
            cmd += '--project "{0}" '.format(query['projection'])

        self.command_list.append(cmd)
        return cmd

    def run_query(self, queries): 
        print("in skyhookrunner run_query")
        for query in queries: 
            cmd = ""
            # Check for WHERE clause 
            if query['table-name']:
                cmd = self.command_template + '--table-name "{0}" '.format(query['table-name'])
            if query['selection']:
                cmd += '--select "{0},{1},{2}" '.format(query['selection'][1],
                                                        query['selection'][0],
                                                        query['selection'][2])
            if query['projection']:
                cmd += '--project "{0}" '.format(query['projection'])
            self.command_list.append(cmd)
        print(cmd)
        return

        results = []
        for cmd in self.command_list:
            res = os.popen(self.prog + cmd).read()
            results.append(res)
        for res in results:
            print(res)
        return results

    def execute_command(self, command):
        results = []
        for cmd in self.command_list:
            res = os.popen(self.prog + cmd).read()
            results.append(res)
        for res in results:
            print(res)
        return results

    def set_command_template(self, options):
        return
