import copy

from cassandra.cqlengine import columns

from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import ModelQuerySet, check_applied, SimpleStatement, conn
from cassandra.cqlengine.functions import Token, BaseQueryFunction
from cassandra.cqlengine.operators import InOperator,  ContainsOperator, BaseWhereOperator
from cassandra.cqlengine.statements import WhereClause, BaseCQLStatement



class AsyncQuerySet(ModelQuerySet):

    async def execute(self, query, params=None, connection=None):
        connection = conn.get_connection(connection)

        if isinstance(query, SimpleStatement):
            pass
        elif isinstance(query, BaseCQLStatement):
            params = query.get_context()
            query = SimpleStatement(
                str(query),
                consistency_level=self._consistency_level,
                fetch_size=query.fetch_size)
        elif isinstance(query, str):
            query = SimpleStatement(query, consistency_level=self._consistency_level)

        result = await self._connection.session.execute_future(
            query, params, timeout=self.timeout)

        return result

    async def _execute_statement(self, statement, connection=None):

        params = statement.get_context()
        s = SimpleStatement(
            str(statement),
            consistency_level=self._consistency_level,
            fetch_size=statement.fetch_size)
        if self.model._partition_key_index:
            key_values = statement.partition_key_values(self.model._partition_key_index)
            if not any(v is None for v in key_values):
                parts = self.model._routing_key_from_values(
                    key_values,
                    conn.get_cluster(connection).protocol_version)
                s.routing_key = parts
                s.keyspace = self.model._get_keyspace()
        connection = connection or self.model._get_connection()
        return await self.execute(s, params, connection=connection)

    async def _async_execute_query(self):
        if self._result_cache is None:
            results = await self._async_execute(self._select_query())
            self._result_generator = (i for i in results)
            self._result_cache = []
            self._construct_result = self._maybe_inject_deferred(self._get_result_constructor())

            if self._materialize_results or self._distinct_fields:
                self._fill_result_cache()

    async def _async_execute(self, statement):
        connection = self._connection or self.model._get_connection()
        result = await self._execute_statement(statement, connection=connection)
        if self._if_not_exists or self._if_exists or self._conditional:
            check_applied(result)
        return result

    async def async_filter(self, *args, **kwargs):
        if len([x for x in kwargs.values() if x is None]):
            raise Exception("None values on filter are not allowed")

        clone = copy.deepcopy(self)
        for operator in args:
            if not isinstance(operator, WhereClause):
                raise Exception('{0} is not a valid query operator'.format(operator))
            clone._where.append(operator)

        for arg, val in kwargs.items():
            col_name, col_op = self._parse_filter_arg(arg)
            quote_field = True

            if not isinstance(val, Token):
                try:
                    column = self.model._get_column(col_name)
                except KeyError:
                    raise Exception("Can't resolve column name: '{0}'".format(col_name))
            else:
                if col_name != 'pk__token':
                    raise Exception("Token() values may only be compared to the 'pk__token' virtual column")

                column = columns._PartitionKeysToken(self.model)
                quote_field = False

                partition_columns = column.partition_columns
                if len(partition_columns) != len(val.value):
                    raise Exception('Token() received {0} arguments but model has {1} partition keys'.format(
                            len(val.value), len(partition_columns)))
                val.set_columns(partition_columns)

            # get query operator, or use equals if not supplied
            operator_class = BaseWhereOperator.get_operator(col_op or 'EQ')
            operator = operator_class()

            if isinstance(operator, InOperator):
                if not isinstance(val, (list, tuple)):
                    raise Exception('IN queries must use a list/tuple value')
                query_val = [column.to_database(v) for v in val]
            elif isinstance(val, BaseQueryFunction):
                query_val = val
            elif (isinstance(operator, ContainsOperator) and
                  isinstance(column, (columns.List, columns.Set, columns.Map))):
                # For ContainsOperator and collections, we query using the value, not the container
                query_val = val
            else:
                query_val = column.to_database(val)
                if not col_op:  # only equal values should be deferred
                    clone._defer_fields.add(column.db_field_name)
                    clone._deferred_values[column.db_field_name] = val  # map by db field name for substitution in results

            clone._where.append(WhereClause(column.db_field_name, operator, query_val, quote_field=quote_field))

        return clone

class BaseCQLWrapper(Model):
    __queryset__ = AsyncQuerySet
    __abstract__ = True

    @classmethod
    async def async_filter(cls, *args, **kwargs):
        return await cls.objects.async_filter(*args, **kwargs)
