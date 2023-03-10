import sqlite3

import pandas as pd
import array
import json
import sys

def create_database(columns, table_name, dbName): 
    conn = sqlite3.connect(f'{dbName}.db')
    cursor = conn.cursor()

    create_table_query = f"CREATE TABLE {table_name} ({','.join([f'{column} {type}' for column, type in columns.items()])} )"
    cursor.execute(create_table_query)


    conn.commit()
    conn.close()

def insertIntoDb(rawData, table_name , rowInfo,dbName):
    conn = sqlite3.connect(f'{dbName}.db')
    cursor = conn.cursor()
    for index , row in rawData.iterrows(): 
        values = []
        for col_name, col_type in rowInfo.items():
            if col_type == "int":
                values.append(int(row[col_name]))
            elif col_type == "float":
                values.append(float(row[col_name]))
            else:
                values.append(row[col_name])
        values_str = ",".join("?" * len(values))
        insert_sql = f"INSERT INTO {table_name} VALUES ({values_str})"
        cursor.execute(insert_sql, tuple(values))
    conn.commit()
    conn.close()
    # preguntar para que county se esta ingresando
    # como un argumento de la llamada -> deberia de ser el nombre de la base de datos 
    # condado nombre del archivo 
def defineIndexes(dbName,table_name, refridFlag):
    conn = sqlite3.connect(f'{dbName}.db')
    cursor = conn.cursor()
    if refridFlag: 
        create_index_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY (parcelnumb_no_formatting)"
        cursor.execute(create_index_query)
    # else: 
    conn.commit()
    conn.close()
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

    print("###############")
    # print('Insert the first file path')
    # input_file_1 = input()
    # print('Insert the second file path')
    # input_file_2 = input()
    print('################ LOADING ##############')
    print('######## PROCESSING  FILE  ########')
    columns_file1 = {}
    dataFile1 = []
    try: 
        df_regrid = pd.read_csv(filePath, low_memory=False)
        for index , row in df_regrid.iterrows(): 
            for col in df_regrid.columns:
                if col in columns_file1:
                    columnType = columns_file1.get(col)
                    currentType = type(row[col]).__name__

                    if columnType == currentType:
                        continue
                    if columnType == 'str':
                        continue
                    if currentType == 'int':
                        continue

                    columns_file1[col] = type(row[col]).__name__
                else:
                    columns_file1[col] = type(row[col]).__name__
        # insertIntoDb(df_regrid, 'ga_clayton_table', columns_file1)
        create_database(columns_file1, tableName , database)
        insertIntoDb(df_regrid, tableName, columns_file1, database)
        # defineIndexes(database,tableName, refridFlag)
    except:
        print('Error while reading the first file')
    print('######## FINISH PROCESSING FILE  ########')
    columns = {}



if __name__ == "__main__":
    readFile()