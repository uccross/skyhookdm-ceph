import os
class SkyhookHandler:
    def __init__(self):
        self.opts = {}
        self.objects = None
        self.query = {}
        self.command_list = []
        self.command_template = ""
        self.skyhook_command = ""
        self.prog = "cd ~/skyhookdm-ceph/build &&"
        self.default_command = 'bin/run-query --num-objs 2 --pool tpchdata --oid-prefix \"public\" --use-cls '

    def check_opts(self):
        self.command = 'bin/run-query ' + '--num-objs ' + str(opts["num-objs"]) + ' --pool ' + opts["pool"] + ' --oid-prefix \"public\" '
        if opts['use-cls']:
            self.command = self.command + '--use-cls '
        if opts['quiet']:
            self.command = self.command + '--quiet '
        return 

    def package_arrow_objects(self):
        return

    def package_flatbuf_objects(self):
        return

    def run_query(self, cmd): 
        # TODO:  Include formatting query here 
        
        results = []
        for cmd in self.command_list:
            res = os.popen(self.prog + self.query).read()
            print(res)
        return results

    
    def clear_previous_query(self):
        self.command_list = []
        return