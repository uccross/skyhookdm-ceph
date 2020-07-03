import os
import logging
import sqlparse
from sqlparse.tokens import Keyword, DML
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis, Comparison

class SkyhookSQLParser():
    def __init__(self, rawUserInput):
        self.rawQuery = rawUserInput
        self.opt_list = None
        self.command = None
        self.command_list = []
        self.default_command = 'bin/run-query --num-objs 2 --pool tpchdata --oid-prefix \"public\" --use-cls '


    def clear_previous_query(self):
        self.command_list = []
        return

    def check_opts(self, opts):
        self.command = 'bin/run-query ' + '--num-objs ' + str(opts["num-objs"]) + ' --pool ' + opts["pool"] + ' --oid-prefix \"public\" '
        if opts['use-cls']:
            self.command = self.command + '--use-cls '
        if opts['quiet']:
            self.command = self.command + '--quiet '
        return
    
    def parse_query(self):
        def extract_query_info(parsed):
            def extract_where(parsed):
                # TODO: Order of allowableOps matters, fix this  
                allowableOps = ['>=', '<=', '!=', '<>','=', '>', '<']
                opsStrDict = {allowableOps[0]: 'geq',
                              allowableOps[1]: 'leq',
                              allowableOps[2]: 'ne',
                              allowableOps[3]: 'ne',
                              allowableOps[4]: 'eq',
                              allowableOps[5]: 'gt',
                              allowableOps[6]: 'lt'}
                matched_op = False
                try:
                    where_tokens = []
                    for item in parsed.tokens:
                        if isinstance(item, Where):
                            for i in item.tokens:
                                if isinstance(i, Comparison):
                                    for op in allowableOps:
                                        if matched_op:
                                            break
                                        if op in str(i):
                                            where_tokens.append(opsStrDict[op])
                                            matched_op = True
                                    where_tokens.append(i.left)
                                    where_tokens.append(i.right)
                                    where_tokens.insert(0, 'WHERE')
                                    return where_tokens
                except:
                    print("Some error occured")
                    pass

               # TODO: Implement compound WHERE clauses below  
               # where_tokens = []
               # identifier = None
               # for item in parsed.tokens:
               #     if isinstance(item, Where):
               #         for i in item.tokens:
               #             if isinstance(i, Comparison):
               #                 print(i.left)
               #             try:
               #                 name = i.get_real_name()
               #                 if name and isinstance(i, Identifier):
               #                     identifier = i
               #                 elif identifier and isinstance(i, Parenthesis):
               #                     sql_tokens.append({
               #                         'key': str(identifier),
               #                         'value': token.value
               #                     })
               #                 elif name:
               #                     identifier = None
               #                     sql_tokens.append({
               #                         'key': str(name),
               #                         'value': u''.join(token.value for token in i.flatten()),
               #                     })
               #                 else:
               #                     get_tokens(i)
               #             except Exception as e:
               #                 print("Passing where token")
               #                 pass

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
            #TODO: Only extract_where if where is present 
            where_list = extract_where(parsed)

            select_list = list(extract_identifiers(select_stream))
            from_list = list(extract_identifiers(from_stream))
            #where_list = list(extract_identifiers(where_list)) #TODO: Make Where extraction a generator function
            return (select_list, from_list, where_list)
        
        def format_query_to_tuple_list(queryInfo):
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
        
        def transform_query():
            statements = sqlparse.split(self.rawQuery)
            for cmd in statements:
                if cmd == '':
                    continue # Skip if query empty to avoid IndexError (temp fix) 
                parsed = sqlparse.parse(cmd)[0]
                queryInfo = extract_query_info(parsed)
                listQuery = format_query_to_tuple_list(queryInfo)
                
                # Check first if WHERE clause exists
                try:
                    if queryInfo[2][0] == 'WHERE':
                        self.command_list.append(self.command + '--table-name "{0}" --select "{1},{2},{3}" --project "{4}"'.format(listQuery[0], queryInfo[2][2], queryInfo[2][1], queryInfo[2][3], listQuery[1]))
                        return
                except:
                    pass
                if listQuery[1] == '':
                    self.command_list.append(self.command + '--table-name "{0}" --project "*"'.format(listQuery[0]))
                else:
                    self.command_list.append(self.command + '--table-name "{0}" --project "{1}"'.format(listQuery[0], listQuery[1]))
            return

        transform_query()
        return

    def exec_query(self, cmd):
        prog = 'cd ~/skyhookdm-ceph/build/ && '
        result = os.popen(prog + cmd).read()
        print(result)
        return result

'''
Entry point of Skyhook SQL Parser.
Instantiates SkyhookSQLParser object and transforms user input
of a SQL query from the client into Skyhook query command. .

Assumptions: 
- No user input is validated. 
- Assumes working in skyhookdm-ceph/src/progly/sql_interface
- Assumes you are working with a physical OSD 
'''
def handle_query(userOpts, rawUserInput):
    assert isinstance(rawUserInput, str), "Expected str"
    assert isinstance(userOpts, dict), "Expected dict"

    skyObj = SkyhookSQLParser(rawUserInput)
    skyObj.check_opts(userOpts)
    skyObj.parse_query()
    result = []
    for cmd in skyObj.command_list:
        result.append(skyObj.exec_query(cmd))
        skyObj.clear_previous_query()
    return result

