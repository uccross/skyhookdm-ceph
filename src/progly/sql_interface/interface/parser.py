import os
import sqlparse
from sqlparse.tokens import Keyword, DML
from sqlparse.sql import IdentifierList, Identifier, Where, Parenthesis, Comparison

class SQLParser():
    def __init__(self):
        '''
        A class that parses SQL statements. 
        '''
        pass

    def parse_query(self, raw_query):
        '''
        A function that parses a SQL string into a dictionary representation. 
        '''
        def extract_sql_semantics(parsed):
            def parse_where_clause(parsed):
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

            def parse_from_clause(parsed):
                    from_seen = False
                    for item in parsed.tokens:
                        if from_seen:
                            if item.ttype is Keyword:
                                return
                            else:
                                yield item
                        elif item.ttype is Keyword and item.value.upper() == 'FROM':
                            from_seen = True

            def parse_select_clause(parsed):
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

            select_stream = parse_select_clause(parsed)
            from_stream = parse_from_clause(parsed)
            where_list = parse_where_clause(parsed)

            select_list = list(extract_identifiers(select_stream))
            from_list = list(extract_identifiers(from_stream))
            #TODO: Make Where extraction a generator function
            #where_list = list(extract_identifiers(where_list)) 

            segments = [select_list, from_list, where_list]

            semantics = []
            
            for s in segments:
                    joined = ', '.join(s)
                    semantics.append(joined)

            return semantics


        sql_statement = sqlparse.split(raw_query)[0]
        
        tokens = sqlparse.parse(sql_statement)[0]

        semantics = extract_sql_semantics(tokens)

        query = {'table-name' : semantics[1],
                 'projection' : semantics[0],
                 'selection'  : semantics[2]}

        return query
