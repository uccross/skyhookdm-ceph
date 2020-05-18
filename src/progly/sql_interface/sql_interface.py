import sqlparse
from sqlparse.tokens import Keyword, DML
from sqlparse.sql import IdentifierList, Identifier
import os

class SkyhookSQLClient:
    def __init__(self):
        self.opt_list = None
        self.command = None
        self.command_list = []
        self.default_command = 'bin/run-query --num-objs 2 --pool tpchdata --oid-prefix \"public\" '

    def clearPreviousQuery(self):
        self.command_list = []
        return

    def checkOpts(self):
        def parseOpts(opts):
            opt_list = opts.split(",")
            if len(opt_list) == 1:
                self.command = self.default_command
                print('Current command: ' + self.command)
                return
            if len(opt_list) > 4:
                raise ValueError('Incorrect number of options set.')
            print(opt_list)
            self.command = 'bin/runquery ' + '--num-objs ' + opt_list[0] + ' --pool ' + opt_list[1]
            print(self.command)

        print("Enter command options as command separated values below. Press 'Enter' without input to use default options.")
        print("Example: [num_objs],[pool_name]")
        opts = input(">>> ")
        parseOpts(opts)
        return
    
    def enterQuery(self):
        def extractQueryInfo(parsed):
            def extract_where(parsed):
                    where_seen = False
                    for item in parsed.tokens:
                        if where_seen:
                            if item.ttype is Keyword:
                                return
                            else:
                                yield item
                        elif item.ttype is Keyword and item.value.upper() == 'WHERE':
                            where_seen = True

            def extract_from(parsed):
                    from_seen = False
                    for item in parsed.tokens:
                        if from_seen:
                            if item.ttype is Keyword:
                                return
                            else:
                                yield item
                        elif item.ttype is Keyword and item.value.upper() == 'FROM':
                            from_seen = True

            def extract_select(parsed):
                from_seen = False
                for item in parsed.tokens:
                    if from_seen:
                        if item.ttype is Keyword:
                            return
                        else:
                            yield item
                    elif item.ttype is DML and item.value.upper() == 'SELECT':
                        from_seen = True

            def extract_identifiers(token_stream):
                for item in token_stream:
                    if isinstance(item, IdentifierList):
                        for identifier in item.get_identifiers():
                            yield identifier.get_name()
                    elif isinstance(item, Identifier):
                        yield item.get_name()

            print(parsed.tokens)
            select_stream = extract_select(parsed)
            from_stream = extract_from(parsed)
            where_stream = extract_where(parsed)

            select_list = list(extract_identifiers(select_stream))
            from_list = list(extract_identifiers(from_stream))
            where_stream = list(extract_identifiers(where_stream))
            return (select_list, from_list, where_stream)
        
        def formatQueryTupleToList(queryTuple):
            listQuery, formattedList = [], []
            listQuery.append(str(queryTuple[1]))
            listQuery.append(str(queryTuple[0]))

            for element in listQuery:
                for char in element:
                    if char in " []'":
                        element = element.replace(char,'')
                formattedList.append(element)
            return formattedList
        
        def transformQuery(rawQuery):
            statements = sqlparse.split(rawQuery)
            print(statements)
            for cmd in statements:
                parsed = sqlparse.parse(cmd)[0]
                queryInfo = extractQueryInfo(parsed)
                listQuery = formatQueryTupleToList(queryInfo)
                if listQuery[1] == '':
                    self.command_list.append(self.command + '--table-name "{0}" --project "*"'.format(listQuery[0]))
                else:
                    self.command_list.append(self.command + '--table-name "{0}" --project "{1}"'.format(listQuery[0], listQuery[1]))
            return

        print("Enter a SQL query (Multiple semi-colon separated queries can be accepted).")
        rawQuery = input(">>> ")
        transformQuery(rawQuery)
        return

    def execQuery(self, cmd):
        print('Executing command: ' + cmd)
        prog = 'cd ~/skyhookdm-ceph && /build/bin/run-query '
        result = os.popen(prog + cmd).read()
        print(result)
        return

'''
Entry point of SQL Client.
Instantiates SkyhookSQLClient object and requests user input
for a SQL query.

Assumptions: 
- SQL query is not validated.
- Assumes working directory contains skyhookdm-ceph repository
- Assumes you are working with a physical OSD 
- Joins don't work correctly yet
'''
def main():
    skObj = SkyhookSQLClient()
    skObj.checkOpts()

    # Run until told otherwise.
    while(1):
        skObj.enterQuery()
        for cmd in skObj.command_list:
            skObj.execQuery(cmd)
            skObj.clearPreviousQuery()

if __name__ == "__main__":
    main()
