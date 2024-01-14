import psycopg2
from sqlalchemy import create_engine

class PostgreSQLManager:
    def __init__(self, host, port, database, user, password):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }

    def create_schema(self, schema_name):
        with self._get_connection() as connection, connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            connection.commit()
            print(f"Schema '{schema_name}' created successfully.")

    def create_table(self, schema_name, table_name, columns):
        with self._get_connection() as connection, connection.cursor() as cursor:
            # Create the table with specified columns
            create_table_query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({', '.join(f'{col[0]} {col[1]}' for col in columns)})"
            cursor.execute(create_table_query)
            connection.commit()
            print(f"Table '{table_name}' in schema '{schema_name}' created successfully.")

    def check_connection_status(self):
        try:
            with self._get_connection() as connection:
                return connection.status == psycopg2.extensions.STATUS_READY
        except psycopg2.OperationalError:
            return False
        
    def get_sqlalchemy_engine(self):
        connection_string = f"postgresql+psycopg2://{self.connection_params['user']}:{self.connection_params['password']}@{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
        return create_engine(connection_string)
    
    def _get_connection(self):
        return psycopg2.connect(**self.connection_params)

# Example usage:
if __name__ == "__main__":
    # Replace these values with your PostgreSQL connection parameters
    host = 'localhost'
    port = '5432'
    database = 'postgres'
    user = 'postgres'
    password = 'postgres'

    # Create an instance of the PostgreSQLManager
    pg_manager = PostgreSQLManager(host, port, database, user, password)

    # Check the database connectivity status
    if pg_manager.check_connection_status():
        print("Database connectivity is active.")
    else:
        print("Unable to connect to the database.")
