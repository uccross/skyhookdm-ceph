import os
import sqlparse
from sqlparse.tokens import Keyword, DML
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis, Comparison

class SQLParser():
    '''
    Parses SQL statements
    '''
    def __init__(self):
        self.raw_query = None

    def parse_query(self, statement):
        def extract_query_info(parsed):
            def extract_where(parsed):
                # TODO: Order of allowed_ops matters, fix this  
                allowed_ops = ['>=', '<=', '!=', '<>','=', '>', '<']
                opts_dict = {allowed_ops[0]: 'geq',
                              allowed_ops[1]: 'leq',
                              allowed_ops[2]: 'ne',
                              allowed_ops[3]: 'ne',
                              allowed_ops[4]: 'eq',
                              allowed_ops[5]: 'gt',
                              allowed_ops[6]: 'lt'}
                matched_op = False
                try:
                    where_tokens = []
                    for item in parsed.tokens:
                        if isinstance(item, Where):
                            for i in item.tokens:
                                if isinstance(i, Comparison):
                                    for op in allowed_ops:
                                        if matched_op:
                                            break
                                        if op in str(i):
                                            where_tokens.append(opts_dict[op])
                                            matched_op = True
                                    where_tokens.append(str(i.left))
                                    where_tokens.append(str(i.right))
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
            where_list = extract_where(parsed)

            select_list = list(extract_identifiers(select_stream))
            from_list = list(extract_identifiers(from_stream))
            #TODO: Make Where extraction a generator function
            #where_list = list(extract_identifiers(where_list)) 

            return (select_list, from_list, where_list)
        
        def format_query_to_tuple_list(query_info):
            listQuery, formattedList = [], []
            query_dict = {}

            # listQuery.append(str(query_info[2]))
            listQuery.append(str(query_info[1]))
            listQuery.append(str(query_info[0]))

            for element in listQuery:
                for char in element:
                    if char in " []'":
                        element = element.replace(char,'')
                formattedList.append(element)

            # TODO: Handle WHERE segment as above 
            formattedList.append(query_info[2])

            return formattedList
        
        def transform_query():
            statements = sqlparse.split(self.raw_query)
            queries = []
            for st in statements:
                if st == '':
                    continue # TODO: Skip EOF (Fix: cut out in client?)
                parsed = sqlparse.parse(st)[0]
                query_info = extract_query_info(parsed)
                list_query = format_query_to_tuple_list(query_info)

                queries.append({'table-name': list_query[0],
                                'projection': list_query[1],
                                'selection': list_query[2]})

            return queries

        self.raw_query = statement
        queries = transform_query()
        return queries