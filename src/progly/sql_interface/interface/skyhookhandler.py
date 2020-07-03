import os
class SkyhookHandler:
    def __init__(self):
        self.options = {'use-cls':  True,
                        'quiet':    False,
                        'pool':     'tpchdata',
                        'num-objs': 2}
        self.command_list = []
        self.command_template = None
        self.prog = "cd ~/skyhookdm-ceph/build && "

    def check_options(self, user_opts):
        # TODO: Handle --use-cls 'False' case. 
        # TODO: Is verbose more readable? (ifel conditions for each option)
        for opt in self.options:
            if user_opts[opt]:
                self.options[opt] = user_opts[opt]
        self.command_template = "bin/run-query --num-objs " + str(self.options['num-objs']) + " \
--pool " + str(self.options["pool"]) + " --oid-prefix \"public\" "
        if self.options["use-cls"]:
            self.command_template += "--use-cls "
        if self.options["quiet"]:
            self.command_template += ("--quiet ")
        return 

    def package_arrow_objects(self):
        return

    def package_flatbuf_objects(self):
        return

    def run_predefined(self, cmd):
        res = os.popen(self.prog + cmd).read()
        return res

    def run_query(self, queries): 
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

        results = []
        for cmd in self.command_list:
            res = os.popen(self.prog + cmd).read()
            results.append(res)
        for res in results:
            print(res)
        return results

    
    def clear_previous_query(self):
        self.command_list = []
        return
