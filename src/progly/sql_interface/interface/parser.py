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
        def parse_clauses(parsed):
            def parse_where_clause(parsed):
                # TODO: Order of allowed_ops matters (ordered by length). Fix this. 
                allowed_ops = ['>=', '<=', '!=', '<>', '=', '>', '<', 'LIKE']

                operator_strs = {allowed_ops[0] : 'geq',
                             allowed_ops[1] : 'leq',
                             allowed_ops[2] : 'ne',
                             allowed_ops[3] : 'ne',
                             allowed_ops[4] : 'eq',
                             allowed_ops[5] : 'gt',
                             allowed_ops[6] : 'lt',
                             allowed_ops[7] : 'like'}

                where_tokens = []
                identifier = None

                for item in parsed.tokens:
                    if isinstance(item, Where):
                        for i in item.tokens:
                            if isinstance(i, Comparison):
                                for op in allowed_ops:
                                    if op in str(i):
                                        where_tokens.append(operator_strs[op])
                                        break
                                    if op.upper() == 'LIKE':
                                        where_tokens.append(operator_strs[op.upper()])
                                where_tokens.append(str(i.left))
                                where_tokens.append(str(i.right))
                
                return where_tokens

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

            select_ids = list(extract_identifiers(select_stream))
            from_ids = list(extract_identifiers(from_stream))
            where_ids = parse_where_clause(parsed)

            identifiers = [select_ids, from_ids, where_ids]

            clauses = []
            for i in identifiers:
                    clauses.append(', '.join(i))

            return clauses


        sql_statement = sqlparse.split(raw_query)[0]
        
        tokenized = sqlparse.parse(sql_statement)[0]

        clauses = parse_clauses(tokenized)

        # TODO: Order should not matter. Fix this. 
        query = {'table-name' : clauses[1],
                 'projection' : clauses[0],
                 'selection'  : clauses[2]}

        return query
