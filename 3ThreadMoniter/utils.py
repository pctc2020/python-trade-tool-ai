import pandas as pd
import mysql.connector
import pymysql
import psycopg2

def insert_data(data, conn):    
    try:
        cur = conn.cursor()
        create_buy_sell_data_table(conn)
        for index, row in data.iterrows():
            # SQL query to insert data into the table
            sql_query = "INSERT INTO buy_sell_data (id, ticker, buy_signal_price, sell_signal_price, strategy_name, indicator, final_trade_date_time, tradestatus) VALUES (NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s')"  % (row['script'], row['buy_signal_price'], row['sell_signal_price'], row['strategy_name'], row['indicator'], row["tradetime2"], row['tradestatus'] )
            print(sql_query)
            cur.execute(sql_query)
            # Execute the query with data parameter
            # Commit the transaction
            conn.commit()
            # Close the cursor and connection
            print("Data inserted successfully!")
    except Exception as e:
        print("Error:", e)

#connecting to database 
def db_connect():
    try:
        print("Connecting to database")
        # MySQL database details
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='Root', database='stockmarket')
        print("Database connected")
        return conn
    except Exception as e:
        print(e)
        return None

# Data update in database table 
def update_to_buy_sell(data):
    try:
        print("Updating results to database")
        conn = db_connect()
        # print(data.columns)
        print(data)
        #insert data in database table
        insert_data(data, conn)
        # data.to_sql("buy_sell_data", conn, if_exists="replace", index=False)
        conn.close()
        print("Update results to database : Completed")
    except Exception as e:
        print(e)

#updata data in new table...
def update_data_table(df):
    # df1 = df.dropna(subset=['sell_signal_price','buy_signal_price'], how='all', axis=0)
    if(df.empty):
        print("Data Cleaning Completed & Data is Empty...")
    else:
        data = df.drop(labels=["stock","open","close","high","low","last_price",
       "prev_close", "quantity", "traded_value", "52W_high",
       "52 Week Low Price"], axis=1)
        update_to_buy_sell(data)

#create table code...
def create_buy_sell_data_table(mydb):
    try:
        cursor = mydb.cursor()
        cursor.execute("SHOW TABLES LIKE 'buy_sell_data'")
        result = cursor.fetchone()
        if result:
            print("Table Already Exist")
        else:
            create_query = """CREATE TABLE buy_sell_data (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                ticker VARCHAR(255),
                                buy_signal_price VARCHAR(255),
                                sell_signal_price VARCHAR(255),
                                strategy_name VARCHAR(255),
                                indicator VARCHAR(255),
                                final_trade_date_time TIMESTAMP,
                                tradestatus VARCHAR(255)
                            )"""
            cursor.execute(create_query)
            print("Table Created Successfully")

        mydb.commit()
        cursor.close()    

    except Exception as e:
        print("Error:", e)

def alg_data_cleanup(indicator, formalname, params, mydb):
    # Connect to the MySQL database
    conn = mysql.connector.connect(**mydb)
    cursor = conn.cursor()
    try:
        # Run the threads and update buy_sell_data table
        # Assuming some processing here to update buy_sell_data

        # If thread fails, delete rows from buy_sell_data based on indicator
        delete_query = f"DELETE FROM buy_sell_data WHERE indicator_name = '{indicator}'"
        cursor.execute(delete_query)
        conn.commit()

        # Update status in formula_range_to_apply table
        update_query = f"UPDATE formula_range_to_apply SET status = 'fail' WHERE formulname = '{formalname}' AND param1 = '{params[0]}' AND param2 = '{params[1]}' AND param3 = '{params[2]}'"
        cursor.execute(update_query)
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        cursor.close()
        conn.close()


# Tradeanalysis_AI

# def save_buy_sell_data_into_database(tempTable, mainTable, mydb):
#     mycursor = mydb.cursor()
#     selQry = "SELECT * FROM " + tempTable 
#     mycursor.execute(selQry) 
#     tempDataFrame = pd.DataFrame(mycursor.fetchall())  # Convert result set to DataFrame
#     tempDataFrame.columns = mycursor.column_names                     
    