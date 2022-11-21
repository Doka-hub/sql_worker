from abc import ABCMeta, abstractmethod

from .mixins import MakeQueryMixin


class DatabaseBase(MakeQueryMixin, metaclass=ABCMeta):

    def __init__(
            self,
            database: str,
            username: str,
            password: str,
            host: str,
            port: int,
    ):
        self.database = database
        self.connect_data = {
            'username': username,
            'password': password,
            'host': host,
            'port': port,
        }
        self.conn = False

    def __enter__(self):
        self.connect()
        self.conn = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn = False

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    def execute_sql(self, sql):
        if not self.conn:
            self.connect()
        print(f'Request completed\n{sql}')
        return []

    @abstractmethod
    def select(self, table: str, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def get(self, table: str, id_: int):
        raise NotImplementedError

    @abstractmethod
    def insert(self, table: str, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def update(self, table: str, id_: int, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def delete(self, table: str, id_: int):
        raise NotImplementedError


class MySQLDatabase(DatabaseBase):
    def __init__(
            self,
            database: str,
            username: str,
            password: str,
            host: str = 'localhost',
            port: int = 3306,
    ):
        super(MySQLDatabase, self).__init__(database, username, password, host, port)
        
    def connect(self):
        print(f'MySQL database "{self.database}" connected!')

    def _value_format(self, value):
        if isinstance(value, str):
            value = f'"{value}"'
        elif isinstance(value, bool):
            value = f'{int(value)}'
        elif isinstance(value, int):
            value = f'{value}'
        elif isinstance(value, type(None)):
            value = f'null'
        return value

    def select(self, table: str, **kwargs):
        sql = f'''
            SELECT * FROM {table} {self._make_where(**kwargs)};
        '''
        return self.execute_sql(sql)

    def get(self, table: str, id_: int):
        return self.select(table, id=id_)

    def insert(self, table: str, **kwargs):
        if kwargs.get('id'):
            del kwargs['id']

        columns = ', '.join(kwargs.keys())
        values = ', '.join(map(self._value_format, kwargs.values()))

        sql = f'''
            INSERT INTO {table} ({columns})
            VALUES ({values});
        '''
        return self.execute_sql(sql)

    def update(self, table: str, id_: int, **kwargs):
        sql = f'''
            UPDATE {table} SET {self._make_data(**kwargs)} WHERE id={id_};
        '''
        return self.execute_sql(sql)

    def delete(self, table: str, id_: int):
        sql = f'''
            DELETE FROM {table} WHERE id={id_};
        '''
        return self.execute_sql(sql)


class PostgreSQLDatabase(DatabaseBase):

    def __init__(
            self,
            database: str,
            username: str,
            password: str,
            host: str = 'localhost',
            port: int = 5432,
    ):
        super(PostgreSQLDatabase, self).__init__(database, username, password, host, port)

    def connect(self):
        print(f'PostgreSQL database "{self.database}" connected!')

    def _value_format(self, value):
        if isinstance(value, str):
            value = f'"{value}"'
        elif isinstance(value, bool):
            value = f'{"true" if value else "false"}'
        elif isinstance(value, int):
            value = f'{value}'
        elif isinstance(value, type(None)):
            value = f'null'
        return value

    def select(self, table: str, **kwargs):
        sql = f'''
            SELECT * FROM {table} {self._make_where(**kwargs)};
        '''
        return self.execute_sql(sql)

    def get(self, table: str, id_: int):
        return self.select(table, id=id_)

    def insert(self, table: str, **kwargs):
        if kwargs.get('id'):
            del kwargs['id']

        columns = ', '.join(kwargs.keys())
        values = ', '.join(map(self._value_format, kwargs.values()))

        sql = f'''
            INSERT INTO {table} ({columns})
            VALUES ({values});
        '''
        return self.execute_sql(sql)

    def update(self, table: str, id_: int, **kwargs):
        sql = f'''
            UPDATE {table} SET {self._make_data(**kwargs)} WHERE id={id_};
        '''
        return self.execute_sql(sql)

    def delete(self, table: str, id_: int):
        sql = f'''
            DELETE FROM {table} WHERE id={id_};
        '''
        return self.execute_sql(sql)
