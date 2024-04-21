import mysql.connector
import Thread_Manager
import pandas as pd

def db_connection():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="Root",
        database="stockmarket"
    )
    return mydb

def df_data(mydb):
    db_cursor=mydb.cursor()
    db_cursor.execute("SELECT * FROM trade_record_20microns")
    trade_records = db_cursor.fetchall()
    # # Get column names from cursor description
    columns = [i[0] for i in db_cursor.description]
    # # Create DataFrame from fetched MySQL data and provide column labels
    df = pd.DataFrame(trade_records, columns=columns)
    # print(df)
    return df
    

def param_data(mydb):
    db_cursor=mydb.cursor()
    db_cursor.execute("SELECT param1, param2, param3, param4, param5, formula_name FROM formula_range_to_apply WHERE status='NOT_STARTED' limit 0, 10")
    db_select=db_cursor.fetchall()
    # print(db_select)
    return db_select

def param_data_siquence(db_select):
    params = []
    filenames = []
    for row in db_select:
        p1 = row[0]
        p2 = row[1]
        p3 = row[2]
        p4 = row[3]
        p5 = row[4]
        prms = [p1, p2, p3, p4, p5]
        # print(prms)
        params.append(prms)
        # flName = "alg_"+row['formula_name']+"."+row['formula_name']+"_buy_sale"
        flName = "alg_"+row[5]+"."+row[5]+"_buy_sale"
        filenames.append(flName.lower())
    # print("001:", trade_record_df.columns)
    return params, filenames

def thread_manager(df, params, filenames, mydb):
    Threads = Thread_Manager.thread_manager(df, params, filenames, mydb)

def main():
    mydb = db_connection()
    print("database Connected!")
    df = df_data(mydb)
    db_select = param_data(mydb)
    params, filenames = param_data_siquence(db_select)
    thread_manager(df, params, filenames, mydb)

main()

