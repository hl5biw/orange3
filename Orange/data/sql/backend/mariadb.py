import logging
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# 
# handler = logging.FileHandler('/home/hevpds/mariadb.log')
# handler.setLevel(logging.DEBUG)
# 
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# 
# logger.addHandler(handler)

import re
import warnings
from contextlib import contextmanager
#from time import time
# from typing import Any, Optional

import mariadb

from Orange.data import ContinuousVariable, DiscreteVariable, StringVariable, TimeVariable, Variable
from Orange.data.sql.backend.base import Backend, ToSql, BackendError


def parse_ex(ex: Exception) -> str:
    try:
        return ex.args[0][1].decode().splitlines()[-1]
    except:  # pylint: disable=bare-except
        return str(ex)

class MariaDBBackend(Backend):
    display_name = "Maria db"

    def __init__(self, connection_params):
      
        super(MariaDBBackend, self).__init__(connection_params)
        
        #collection serve : port error
        if 'port' in connection_params:
            port = connection_params['port']
            if isinstance(port, str) and port.isdigit():
                connection_params['port'] = int(port)
        else:
            connection_params['port'] = 3306
        
        self.connection_params = connection_params
        self.connect()

    def connect(self):
        self.connection = mariadb.connect(**self.connection_params)
        self.cursor = self.connection.cursor()
    
    # from postgre    
    def list_tables_query(self, schema=None):
        if schema:
            schema_clause = f"AND TABLE_SCHEMA = '{schema}'"
        else:
            schema_clause = ""
    
        return f"""SELECT TABLE_SCHEMA AS "Schema",
                          TABLE_NAME AS "Name"
                   FROM information_schema.TABLES
                   WHERE TABLE_TYPE = 'BASE TABLE'
                         {schema_clause}
                   ORDER BY TABLE_SCHEMA, TABLE_NAME;"""
    
    def quote_identifier(self, name):
        """Quote identifier name so it can be safely used in queries

        Parameters
        ----------
        name: str
            name of the parameter

        Returns
        -------
        quoted parameter that can be used in sql queries
        """
        return "`{}`".format(name.replace("`", "``"))

    def unquote_identifier(self, quoted_name):
        """Remove quotes from identifier name
        Used when sql table name is used in where parameter to
        query special tables

        Parameters
        ----------
        quoted_name : str

        Returns
        -------
        unquoted name
        """
        return quoted_name.strip('`')
    

    def create_sql_query(self, table_name, fields, filters=(), group_by=None, order_by=None, 
                        offset=None, limit=None, use_time_sample=None):
        """Construct an sql query using the provided elements.
        Parameters
        ----------
        table_name : str
        fields : List[str]
        filters : List[str]
        group_by: List[str]
        order_by: List[str]
        offset: int
        limit: int
        use_time_sample: int
        Returns
        -------
        string containing sql query
        """
        # logger.debug("def create_sql_query()")
        # logger.debug(fields)
        fields_str = ", ".join(fields)
        query = f"SELECT {fields_str} FROM {table_name}"

        if filters:
            filters_str = " AND ".join(filters)
            query += f" WHERE {filters_str}"

        if group_by:
            group_by_str = ", ".join(group_by)
            query += f" GROUP BY {group_by_str}"

        if order_by:
            order_by_str = ", ".join(order_by)
            query += f" ORDER BY {order_by_str}"

        if offset is not None:
            query += f" OFFSET {offset}"

        if limit is not None:
            query += f" LIMIT {limit}"
            
        if use_time_sample is not None:
            query += f" SAMPLE {use_time_sample} SECOND"
            
        # logger.debug("return query")
        # logger.debug(query)
        
        return query
    
    @contextmanager
    def execute_sql_query(self, query, params=None):
        """Context manager for execution of sql queries
        Usage:
            ```
            with backend.execute_sql_query("SELECT * FROM foo") as cur:
                cur.fetch_all()
            ```
        Parameters
        ----------
        query : string
            query to be executed
        params: tuple
            parameters to be passed to the query
        Returns
        -------
        yields a cursor that can be used to access the data
        """      
        # logger.debug("execute_sql_query")
        # logger.debug(query)
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                yield cursor
                # result = cursor.fetchall()  # 쿼리 결과 가져오기
                # logger.debug(f"Query executed successfully. Result: {result}")
        

        except mariadb.Error as ex:
            # logger.error(f"Query execution failed. Query: {query}, Params: {params}, Error: {ex}")
            raise BackendError(parse_ex(ex)) from ex


    def create_variable(self, field_name, field_metadata,
                        type_hints, inspect_table=None):
        if field_name in type_hints:
            var = type_hints[field_name]
        else:
            var = self._guess_variable(field_name, field_metadata,
                                       inspect_table)
    
        field_name_q = self.quote_identifier(field_name)
        if var.is_continuous:
            if isinstance(var, TimeVariable):
                var.to_sql = ToSql("UNIX_TIMESTAMP({})"
                                   .format(field_name_q))
            else:
                var.to_sql = ToSql("({})+0"
                                   .format(field_name_q))
        else:  # discrete or string
            var.to_sql = ToSql("({})".format(field_name_q))
                               

        return var



    def _guess_variable(self, field_name, field_metadata, inspect_table):
        type_code = field_metadata[0]
        
        FLOATISH_TYPES = (4, 5, 8, 246)  # float, double, decimal
        INT_TYPES = (1, 2, 3, 8, 9, 13, 16)  # integer, smallint, tinyint, mediumint, bigint, bit, boolean
        CHAR_TYPES = (253, 254, 246)  # varchar, char, decimal
        BOOLEAN_TYPES = (16,)  # boolean
        DATE_TYPES = (10, 11, 12, )  # date, datetime, timestamp
        TIME_TYPES = (9, 11, 12, 15, 16)  # time, datetime, timestamp, year

        if type_code in FLOATISH_TYPES:
            return ContinuousVariable.make(field_name)

        if type_code in TIME_TYPES + DATE_TYPES:
            tv = TimeVariable.make(field_name)
            tv.have_date |= type_code in DATE_TYPES
            tv.have_time |= type_code in TIME_TYPES
            return tv

        if type_code in INT_TYPES:   # integer, smallint, tinyint, mediumint, bigint, bit, boolean
            if inspect_table:
                values = self.get_distinct_values(field_name, inspect_table)
                if values:
                    return DiscreteVariable.make(field_name, values)
            return ContinuousVariable.make(field_name)

        if type_code in BOOLEAN_TYPES:
            return DiscreteVariable.make(field_name, ['0', '1'])

        if type_code in CHAR_TYPES:
            if inspect_table:
                values = self.get_distinct_values(field_name, inspect_table)
                # remove trailing spaces
                values = [v.rstrip() for v in values]
                if values:
                    return DiscreteVariable.make(field_name, values)
            # logger.debug("No distinct values found for field {}".format(field_name))
    
        # logger.debug("Unknown variable type for field {}".format(field_name))
        return StringVariable.make(field_name)

    #from postgree
    def distinct_values_query(self, field_name: str, table_name: str) -> str:
        fields = ["`{}`".format(field_name)]
        return self.create_sql_query(
            table_name, fields, group_by=fields, order_by=fields, limit=21
    )
