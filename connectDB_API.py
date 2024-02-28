import pandas as pd
import psycopg2

class connection_API():
    def __init__(self, database_name="postgres"):
        self.credentials = pd.read_json("credentials.json")
        user_name = self.credentials["user_name"][0]
        password = self.credentials["password"][0]

        self.conn = psycopg2.connect(
            dbname=database_name,
            user=user_name,
            password=password,
            host="localhost",
            port="5432"
        )
        self.cur = self.conn.cursor()
    
    def create_schema(self, schema_name):
        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print("Schema ",schema_name, " is successfully created!")

    def create_table(self, schema_name, table_name, dataset):

        create_table_query = f"CREATE TABLE {schema_name}.{table_name} ("

        columns = list(dataset.columns)
        types = dataset.dtypes

        i = 0

        while i < len(columns):
            if "int" in str(types[i]):
                create_table_query += f"{columns[i]} INTEGER,"
            elif "float" in str(types[i]):
                create_table_query += f"{columns[i]} FLOAT,"
            else:
                create_table_query += f"{columns[i]} VARCHAR(255),"
            i = i + 1

        create_table_query = create_table_query[:-1] + ");"

        print("Table ",schema_name + "." + table_name, " is successfully created!")
    
    def commit_close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()