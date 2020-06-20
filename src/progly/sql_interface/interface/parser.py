import os
import logging
import sqlparse
from sqlparse.tokens import Keyword, DML
from sqlparse.sql import IdentifierList, Identifier

class SkyhookSQLParser():
    def __init__(self, rawUserInput):
        self.rawQuery = rawUserInput
        self.opt_list = None
        self.command = None
        self.command_list = []
        self.default_command = 'bin/run-query --num-objs 2 --pool tpchdata --oid-prefix \"public\" --use-cls '


    def clearPreviousQuery(self):
        self.command_list = []
        return

    def checkOpts(self, opts):
        self.command = 'bin/run-query ' + '--num-objs ' + str(opts["num_objs"]) + ' --pool ' + opts["pool"] + ' --oid-prefix \"public\" '
        if opts['cls']:
            self.command = self.command + '--use-cls '
        if opts['quiet']:
            self.command = self.command + '--quiet '
        return
    
    def parseQuery(self):
        def extractQueryInfo(parsed):
            # def extract_where(parsed):
            #         where_seen = False
            #         for item in parsed.tokens:
            #             if where_seen:
            #                 if item.ttype is Where:
            #                     return
            #                 else:
            #                     yield item
            #             elif item.ttype is Where and item.value.upper() == 'WHERE':
            #                 where_seen = True

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
                select_seen = False
                for item in parsed.tokens:
                    if select_seen:
                        if item.ttype is Keyword:
                            return
                        else:
                            yield item
                    elif item.ttype is DML and item.value.upper() == 'SELECT':
                        select_seen = True

            def extract_identifiers(token_stream):
                for item in token_stream:
                    if isinstance(item, IdentifierList):
                        for identifier in item.get_identifiers():
                            yield identifier.get_name()
                    elif isinstance(item, Identifier):
                        yield item.get_name()

            select_stream = extract_select(parsed)
            from_stream = extract_from(parsed)
            # where_stream = extract_where(parsed)

            select_list = list(extract_identifiers(select_stream))
            from_list = list(extract_identifiers(from_stream))
            #where_stream = list(extract_identifiers(where_stream))
            return (select_list, from_list) # where_stream)
        
        def formatQueryTupleToList(queryInfo):
            listQuery, formattedList = [], []
            # listQuery.append(str(queryInfo[2]))
            listQuery.append(str(queryInfo[1]))
            listQuery.append(str(queryInfo[0]))

            for element in listQuery:
                for char in element:
                    if char in " []'":
                        element = element.replace(char,'')
                formattedList.append(element)
            return formattedList
        
        def transformQuery():
            statements = sqlparse.split(self.rawQuery)
            for cmd in statements:
                if cmd == '':
                    continue # Skip if query empty to avoid IndexError (temp fix) 
                parsed = sqlparse.parse(cmd)[0]
                queryInfo = extractQueryInfo(parsed)
                listQuery = formatQueryTupleToList(queryInfo)
                if listQuery[1] == '':
                    self.command_list.append(self.command + '--table-name "{0}" --project "*"'.format(listQuery[0]))
                else:
                    self.command_list.append(self.command + '--table-name "{0}" --project "{1}"'.format(listQuery[0], listQuery[1]))
            return

        transformQuery()
        return

    def execQuery(self, cmd):
        prog = 'cd ~/skyhookdm-ceph/build/ && '
        result = os.popen(prog + cmd).read()
        print(result)
        return

'''
Entry point of Skyhook SQL Parser.
Instantiates SkyhookSQLParser object and transforms user input
of a SQL query from the client into Skyhook query command. .

Assumptions: 
- No user input is validated. 
- Assumes working in skyhookdm-ceph/src/progly/sql_interface
- Assumes you are working with a physical OSD 
'''
def handleQuery(userOpts, rawUserInput):
    assert isinstance(rawUserInput, str), 'Expected str'
    assert isinstance(userOpts, dict), 'Expected dict'

    skyObj = SkyhookSQLParser(rawUserInput) 
    skyObj.checkOpts(userOpts)
    skyObj.parseQuery()
    for cmd in skyObj.command_list:
        skyObj.execQuery(cmd)
        skyObj.clearPreviousQuery()
