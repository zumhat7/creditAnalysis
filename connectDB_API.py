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
        """
        Parameters:
            shema_name = <class 'str'>
        Goal:
            creates a new schema with the given name in DB
        """
        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print("Schema ",schema_name, " is successfully created!")

    def create_table(self, schema_name, table_name, dataset):
        """
        Parameters:
            shema_name = <class 'str'>
            table_name = <class 'str'>
            dataset = <class 'pandas.core.frame.DataFrame'>
        Goal:
            creates a blank table with columns of given dataset in the schema
        """

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

        table_exists = self.check_if_exists(schema_name, table_name)

        if not table_exists:
            self.cur.execute(create_table_query)
            print("Table ",schema_name + "." + table_name, " is successfully created!")
        else:
            print("Table ",schema_name + "." + table_name, " is already created!")


    def data_import(self, schema_name, table_name, dataset):
        """
        Parameters:
            shema_name = <class 'str'>
            table_name = <class 'str'>
            dataset = <class 'pandas.core.frame.DataFrame'>
        Goal:
            imports data from dataset to existing table in schema
        """

        column_names = list(dataset.columns)
        insert_query = f"INSERT INTO {schema_name}.{table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s' for _ in column_names])})"

        for row in dataset.itertuples(index=False):
            self.cur.execute(insert_query, row)
        print("Table ",schema_name + "." + table_name, " is successfully imported!")
        #def query(self, query):

    def data_query(self, query):
        """
        Parameters:
            query = <class 'str'>
        Goal:
            exports data from database table into a pandas DataFrame
        Returns:
            pandas DataFrame
        """

        self.cur.execute(query)
        rows = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]  # Sütun adlarını al

        return pd.DataFrame(rows, columns=columns)
    
    def check_if_exists(self, schema_name, table_name):
        """
        Parameters:
            shema_name = <class 'str'>
            table_name = <class 'str'>
        Goal:
            get the boolean value if the given table is exists in given schema
        Returns:
            a boolean value
        """
                
        check_table_query = f"""SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables 
            WHERE  table_schema = '{schema_name}'
            AND    table_name = '{table_name}'
            );"""
        
        self.cur.execute(check_table_query)
        table_exists = self.cur.fetchone()[0]
        return table_exists
    
    def commit_close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()