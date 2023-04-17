import sqlite3
import os
import pandas as pd
import sys
import math

dbTypes = {
    "int" : "INTEGER",
    "str" : "TEXT",
    "float" : "REAL"
}

def create_database(columns, table_name, dbName, flag, columnParams): 
    try:
        conn = sqlite3.connect(f'{dbName}.db')
        cursor = conn.cursor()

        if flag == 'true':
            create_table_query = f"CREATE TABLE {table_name} ({','.join([f'{column} {type}' for column, type in columns.items()])}, PRIMARY KEY (parcelnumb_no_formatting))"
            # create_table_query = f"CREATE TABLE {table_name} ({','.join([f'{column} {type}' for column, type in columns.items()])})"
            cursor.execute(create_table_query)
            create_index_query = f"CREATE INDEX index_parcelnumb ON {table_name} (parcelnumb);"
            cursor.execute(create_index_query)
        else: 
            create_table_query = f"CREATE TABLE {table_name} ({','.join([f'{column} {type}' for column, type in columns.items()])})"
            cursor.execute(create_table_query)
            splitParameters = columnParams.split(',')   
            for item in splitParameters:
                if '$' in item:
                    create_index_query_param = f"CREATE INDEX index_{item.replace('$','')} ON {table_name} ({item.replace('$','')});"
                    print(create_index_query_param)
                    cursor.execute(create_index_query_param)
        conn.commit()
        conn.close()

    except sqlite3.OperationalError as e:
        print('Error on create database: ',e)

def insertIntoDb(rawData, table_name , rowInfo,dbName ,flag, columnParams):
    filterColumns = columnParams.replace('$','').split(',')
    try:
        conn = sqlite3.connect(f'{dbName}.db')
        cursor = conn.cursor()
        for index , row in rawData.iterrows(): 
            values = []
            for col_name, col_type in rowInfo.items():
                if flag == 'false':
                    if col_name in filterColumns:
                        if col_type == "INTEGER":
                            values.append(int(row[col_name]))
                        elif col_type == "REAL":
                            values.append(float(row[col_name]))
                        else:
                            values.append(row[col_name])
                    else: 
                        print("Couldn't process file because some required fields are missing")
                        return
                else:
                    if col_type == "INTEGER":
                        values.append(int(row[col_name]))
                    elif col_type == "REAL":
                        values.append(float(row[col_name]))
                    else:
                        values.append(row[col_name])
            values_str = ",".join("?" * len(values))
            insert_sql = f"INSERT OR REPLACE INTO {table_name} VALUES ({values_str})"
            cursor.execute(insert_sql, tuple(values))
        conn.commit()
        conn.close()

    except sqlite3.OperationalError as e:
        print('Error on insert into db: ', e)
    # preguntar para que county se esta ingresando
    # como un argumento de la llamada -> deberia de ser el nombre de la base de datos 
    # condado nombre del archivo 
def defineIndexes(dbName,table_name, refridFlag,columnParams):
    try:
        conn = sqlite3.connect(f'{dbName}.db')
        cursor = conn.cursor()
        if refridFlag == 'true': 
            create_primary_index_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY (parcelnumb_no_formatting);"
            create_index_query = f"CREATE INDEX my_index ON {table_name} (parcelnumb);"
            cursor.execute(create_primary_index_query)
            cursor.execute(create_index_query)                      
        else:    
            splitParameters = columnParams.split(',')   
            for item in splitParameters:
                if '$' in item:
                    print('INDEX FOUND', item)
                else: 
                    print('NORMAL COLUMN ',item)
        conn.commit()
        conn.close()
    except sqlite3.OperationalError as e:
        print('Error while defininig indexes on: ',e)
def readFile():
    database = sys.argv[1]
    print('Database name:', database)

    # Get the second command-line argument
    filePath = sys.argv[2]
    print('FilePath:', filePath)

    # Get the third command-line argument
    tableName = sys.argv[3]
    print('Table name:', tableName)

    # get the fourth command-line argument
    refridFlag = sys.argv[4]
    print('Flag:', refridFlag)

    # get the fifth command-line argument
    columnParams = ''
    if refridFlag:
        if refridFlag == 'false':
            columnParams = sys.argv[5]
            print('Columns definition:', columnParams)


    print("###############")
    dbExist = os.path.exists(f'{database}.db')
   
    # print('Insert the first file path')
    # input_file_1 = input()
    # print('Insert the second file path')
    # input_file_2 = input()
    print('################ LOADING ##############')
    print('######## PROCESSING  FILE  ########')
    columns_file1 = {}
    filterColumns = columnParams.replace('$','').split(',')
    try:
        df_regrid = pd.read_csv(filePath, low_memory=False)
        for index , row in df_regrid.iterrows(): 
            for col in df_regrid.columns:
                if refridFlag =='false' and not col in filterColumns:
                    continue
                currentType = 'TEXT' if pd.isna(row[col]) else dbTypes[type(row[col]).__name__]
                if col in columns_file1:
                    columnType = columns_file1.get(col)
                    if col == 'parcelnumb_no_formatting':
                        continue
                    if columnType == currentType:
                        continue
                    if columnType == 'TEXT':
                        columns_file1[col] = currentType
                        continue
                    if currentType == 'INTEGER':
                        continue

                    columns_file1[col] = currentType
                else:
                    if col == 'parcelnumb_no_formatting':
                        columns_file1[col] = 'TEXT'
                        continue
                    columns_file1[col] = currentType
        # insertIntoDb(df_regrid, 'ga_clayton_table', columns_file1)
        if not dbExist:
            create_database(columns_file1, tableName , database, refridFlag, columnParams)
        # defineIndexes(database,tableName, refridFlag, columnParams)
        insertIntoDb(df_regrid, tableName, columns_file1, database, refridFlag, columnParams)
        
    except sqlite3.OperationalError as e:
        print('Error while reading the first file: ', e)
    print('######## FINISH PROCESSING FILE  ########')



if __name__ == "__main__":
    readFile()

#A      B       C
#1      1.4     3
#JF     1       1.4
#1.4    3       3

#A      B       C
#int    float   int
#str    float   float
#
