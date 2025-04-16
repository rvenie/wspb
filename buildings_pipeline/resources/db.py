import os
import psycopg2
from psycopg2.extras import DictCursor
from dagster import resource, Field

class PostgreSQLResource:
    def __init__(self, host, port, database, user, password, schema=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        self._conn = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        try:
            self._conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if self.schema:
                with self._conn.cursor() as cursor:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
                    self._conn.commit()
        except Exception as e:
            raise Exception(f"Ошибка подключения к PostgreSQL: {e}")
    
    def get_connection(self):
        if self._conn is None or self._conn.closed:
            self._initialize_connection()
        return self._conn
    
    def execute_query(self, query, params=None, fetch=False):
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"Ошибка выполнения запроса: {e}")
    
    def insert_dataframe(self, df, table_name, schema=None, if_exists='fail'):
        """
        Вставляет DataFrame в таблицу PostgreSQL
        
        Args:
            df: pandas DataFrame для вставки
            table_name: Имя таблицы
            schema: Имя схемы (опционально)
            if_exists: Поведение, если таблица существует ('fail', 'replace', 'append')
        """
        from io import StringIO
        import pandas as pd
        
        conn = self.get_connection()
        
        # Определяем полное имя таблицы
        full_table_name = f"{schema or self.schema or 'public'}.{table_name}"
        
        # Проверяем, существует ли таблица
        table_exists = False
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{schema or self.schema or 'public'}' 
                    AND table_name = '{table_name}'
                );
            """)
            table_exists = cursor.fetchone()[0]
        
        # Обрабатываем параметр if_exists
        if table_exists:
            if if_exists == 'fail':
                raise ValueError(f"Таблица {full_table_name} уже существует")
            elif if_exists == 'replace':
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE {full_table_name}")
                    conn.commit()
                    table_exists = False
            elif if_exists == 'append':
                pass
            else:
                raise ValueError(f"Недопустимое значение для if_exists: {if_exists}")
        
        # Создаем таблицу, если она не существует
        if not table_exists:
            # Сопоставление типов pandas с типами PostgreSQL
            dtype_mapping = {
                'int64': 'BIGINT',
                'float64': 'DOUBLE PRECISION',
                'bool': 'BOOLEAN',
                'datetime64[ns]': 'TIMESTAMP',
                'object': 'TEXT'
            }
            
            columns = []
            for col, dtype in zip(df.columns, df.dtypes):
                pg_type = dtype_mapping.get(str(dtype), 'TEXT')
                columns.append(f"\"{col}\" {pg_type}")
            
            create_table_query = f"CREATE TABLE {full_table_name} ({', '.join(columns)})"
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()
        
        # Подготавливаем данные для копирования
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=True, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        # Копируем данные в таблицу
        with conn.cursor() as cursor:
            cursor.copy_expert(
                f"COPY {full_table_name} ({','.join([f'\"{c}\"' for c in df.columns])}) FROM STDIN WITH CSV HEADER DELIMITER E'\t' NULL '\\N'",
                buffer
            )
            conn.commit()
        
        return True
    
    def close(self):
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            self._conn = None

@resource(
    config_schema={
        "host": Field(str, description="Хост базы данных", default_value="localhost"),
        "port": Field(int, description="Порт базы данных", default_value=5432),
        "database": Field(str, description="Имя базы данных"),
        "user": Field(str, description="Пользователь базы данных"),
        "password": Field(str, description="Пароль базы данных"),
        "schema": Field(str, description="Схема базы данных", is_required=False),
    }
)
def postgres_resource(init_context):
    return PostgreSQLResource(
        host=init_context.resource_config["host"],
        port=init_context.resource_config["port"],
        database=init_context.resource_config["database"],
        user=init_context.resource_config["user"],
        password=init_context.resource_config["password"],
        schema=init_context.resource_config.get("schema")
    )
