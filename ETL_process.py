import pandas as pd
import connectDB_API
import ast
class ETL_process():
    def __init__(self, dataset, categorize_columns, new_table_names, column_to_table, id_column_name):
        self.dataset = dataset
        #print(df_customers.info())
        self.categorize_columns = categorize_columns
        self.new_table_names = new_table_names
        self.column_to_table = column_to_table
        self.id_column_name = id_column_name
    
    def data_manipulation(self):

        for column in self.new_table_names:
            try:
                self.dataset[column] = self.dataset[column].apply(str).str.replace("'",'"')
                self.dataset[column] = self.dataset[column].apply(lambda x: ast.literal_eval(x))
            except:
                k = 0

        datasets = []
        x = 0

        for table_name in self.new_table_names:
            if table_name in self.column_to_table:
                dataset = self.dataset[[self.id_column_name,table_name]]
                #print(x, "---->", table_name)
            elif table_name in list(self.categorize_columns.keys()):
                dataset = self.dataset[self.categorize_columns[table_name]]
                #print(x, "---->", table_name)
            else:
                #print(x, "---->", table_name)
                value_sets = []
                id_sets = []
                #print(table_name, "--->", type(df_customers[table_name][0]), type([]), type(df_customers[table_name][0]) == type([]))
                for i, row in self.dataset.iterrows():
                    value = row[table_name]
                    if type(value) == type([]):
                        #print(table_name, "--->", type(value), type([]), type(value) == type([]))
                        for item in value:
                            value_sets.append(item)
                            id_sets.append(row[self.id_column_name])
                    else:
                        value_sets.append(value)
                        id_sets.append(row[self.id_column_name])
                dataset = pd.DataFrame(data=value_sets, index=id_sets)
                column_names = list(dataset.columns)
                dataset.reset_index(inplace = True)
                dataset.columns = [self.id_column_name] + column_names
            for column in list(dataset.columns):
                try:
                    dataset[column] = dataset[column].astype(int)
                except:
                    try:
                        dataset[column] = dataset[column].astype(float)
                    except:
                        dataset[column] = dataset[column].astype(str)
            datasets.append(dataset)
            x = x + 1
        return datasets 
    
    def data_import(self, shema_name,db_name):
        """
        schema_name: <str>
        table_name: <str>
        """
        conn = connectDB_API.connection_API(database_name = db_name)
        conn.create_schema(shema_name)

        datasets = self.data_manipulation()

        j = 0
        for table_name in self.new_table_names:
            conn.create_table(shema_name,table_name, datasets[j])
            conn.data_import(shema_name,table_name, datasets[j])
            j = j + 1
            #dataset.index.name = table_name
        
        conn.commit_close()
