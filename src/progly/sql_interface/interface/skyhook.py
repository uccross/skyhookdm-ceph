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
