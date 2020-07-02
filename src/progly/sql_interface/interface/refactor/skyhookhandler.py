import os
class SkyhookHandler:
    def __init__(self):
        self.options = {'use-cls':  True,
                        'quiet':    False,
                        'pool':     'tpchdata',
                        'num-objs': 2}
        self.sql_queries = None
        self.command_list = []
        self.command_template = "bin/run-query --num-objs _ --pool _ --oid-prefix \"public\""
        self.prog = "cd ~/skyhookdm-ceph/build &&"
        self.default_command = 'bin/run-query --num-objs 2 --pool tpchdata \
                               --oid-prefix \"public\" --use-cls '
#        self.command = 'bin/run-query ' + '--num-objs ' + str(options["num-objs"]) + '\
 #                      --pool ' + options["pool"] + ' --oid-prefix \"public\" '

                               

    def check_options(self, user_opts):
        # TODO: Handle --use-cls 'False' case. 
        # TODO: Is verbose more readable? (ifel conditions for each option)
        for opt in self.options:
            if user_opts[opt]:
                self.options[opt] = user_opts[opt]
        print(self.options)
        return 

    def package_arrow_objects(self):
        return

    def package_flatbuf_objects(self):
        return

    def run_query(self, queries): 
        for query in queries: 
            print(query)

        self.sql_queries = queries
        results = []
        for cmd in self.command_list:
            res = os.popen(self.prog + self.query).read()
            print(res)
        return results

    
    def clear_previous_query(self):
        self.command_list = []
        return
