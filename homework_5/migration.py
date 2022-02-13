import psycopg2
import os


class Migration:
    def __init__(self, conn_string_source, conn_string_target, result_folder='results'):
        self.conn_string_source = conn_string_source
        self.conn_string_target = conn_string_target
        self.result_folder = result_folder
        self.conn_source = None
        self.conn_target = None

    def connect_source(self):
        self.conn_source = psycopg2.connect(self.conn_string_source)

    def close_source_conn(self):
        self.conn_source.close()

    def close_target_conn(self):
        self.conn_target.close()

    def __check_target_connection(self):
        if self.conn_target is None:
            self.connect_target()

    def __check_source_connection(self):
        if self.conn_source is None:
            self.connect_source()

    def connect_target(self):
        self.conn_target = psycopg2.connect(self.conn_string_target)

    def download_data_by_tables(self, tables):
        self.__check_source_connection()

        with self.conn_source.cursor() as cursor:
            for table in tables:
                q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
                filename = f"./{self.result_folder}/{table}.csv"
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                with open(filename, 'w', encoding='utf-8') as f:
                    cursor.copy_expert(q, f)

    def load_data_by_tables(self, tables):
        self.__check_target_connection()

        with self.conn_target.cursor() as cursor:
            for table in tables:
                q = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
                filename = f"./{self.result_folder}/{table}.csv"
                with open(filename, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(q, f)

    def check_loaded_data_by_tables(self, tables):
        self.__check_source_connection()
        with self.conn_source.cursor() as cursor:
            print("-----Source Database-----")
            for table in tables:
                cursor.execute(f'select count(*) from {table}')
                count = cursor.fetchall()
                print(f"{table} count: {count}")

        self.__check_target_connection()
        with self.conn_target.cursor() as cursor:
            print("-----Target Database-----")
            for table in tables:
                cursor.execute(f'select count(*) from {table}')
                count = cursor.fetchall()
                print(f"{table} count: {count}")
