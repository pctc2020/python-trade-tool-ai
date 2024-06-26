import time
import pandas as pd
import T5threadmanager
import mysql.connector

def db_connection():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="Root",
        database="stockmarket"
    )
    return mydb


def getALLToTradeDataFromBuySaleTable(mydb, tradestatus):
    db_cursor=mydb.cursor()
    db_cursor.execute("select * from buy_sell_data where tradestatus='"+tradestatus+"'")
    data = db_cursor.fetchall()
    # # Get column names from cursor description
    columns = [i[0] for i in db_cursor.description]
    # # Create DataFrame from fetched MySQL data and provide column labels
    df = pd.DataFrame(data, columns=columns)
    # print(df)
    return df

def insert_data_to_trade_data_table(df, mydb):    
    try:
        cursor = mydb.cursor()
        # Create a timestamp in milliseconds
        current_time_millis = int(time.time() * 1000)
        # create_trade_data_table(mydb)
        for index, row in df.iterrows():
            # SQL query to insert data into the table
            query = """INSERT INTO trade_data (id, indicator, strategy_name, final_trade_date_time, ticker, qty, buy_signal_price, sell_signal_price, tradestatus) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""  
            values = (current_time_millis, row['indicator'], row['strategy_name'], row['final_trade_date_time'], row['ticker'], 10, row['buy_signal_price'], row['sell_signal_price'], "ORDER_PLACED")
            # print(query)
            cursor.execute(query, values)
            mydb.commit()
            print("Data inserted successfully!")
        cursor.close()
    except Exception as e:
        print("Error:", e)


#create table code...
def create_trade_data_table(mydb):
    try:
        cursor = mydb.cursor()
        cursor.execute("SHOW TABLES LIKE 'trade_data'")
        result = cursor.fetchone()
        if result:
            print("Table Already Exist")
        else:
            create_query = """CREATE TABLE trade_data (
                                id BIGINT,
                                indicator VARCHAR(255),
                                strategy_name VARCHAR(255),
                                final_trade_date_time TIMESTAMP,
                                ticker VARCHAR(255),
                                qty int,
                                buy_signal_price FLOAT,
                                sell_signal_price FLOAT,
                                tradestatus VARCHAR(255)
                            )"""
            cursor.execute(create_query)
            print("Table Created Successfully")

        mydb.commit()
        cursor.close()    

    except Exception as e:
        print("Error:", e)

def main():
    try:
        mydb = db_connection()
        tradestatus = 'Completed' 
        df = getALLToTradeDataFromBuySaleTable(mydb, tradestatus)
        create_trade_data_table(mydb)
        insert_data_to_trade_data_table(df, mydb)
        print(" insert trade record into trade table : {} ".format(len(df)))
        T5threadmanager.update_trade_status("ORDER_PLACED", "TRADE_SUCCESS", mydb)
        print(" updat status with trade success : {} ".format(len(df)))
    except Exception as e:
        print("Error:", e)
    finally:
        if mydb:
            mydb.close()

if __name__ == "__main__":
    main()


















# #--------------------------------------------------------------------------------------
# # mysql connection 
# mydb = mysql.connector.connect(
#   host="localhost",
#   user="root",
#   password="admin",
#   database="shop_portal"
# )


# def db_connect():
#   try:
#     print("Connecting to database")
#     db_connection_str = "mysql+pymysql://root:admin@localhost:3306/shop_portal"
#     db_connection = create_engine(db_connection_str)
#     print("databse connected")
#     return db_connection
#   except Exception as e:
#     print(e,e.args)
#     return None

# def getALLToTradeDataFromBuySaleTable(tradestatus):
#     selQry = "select * from buy_sell_data where tradestatus='"+tradestatus+"'"
#     dataframe =  pd.read_sql(selQry, db_connect())
#     return dataframe


# def prepareTradeDF(toTradeBuySaleDF):
#   tradeDf = pd.DataFrame(columns=['id', 'indicator', 'strategy_name', 'ticker', 'qty', 
#     'final_trade_date_time', 'buy_signal_price', 'sell_signal_price'])
#   for index2, buySaleRow in toTradeBuySaleDF.iterrows():
#     new_row = { 'id': time.time()*1000.0,
#         'indicator': buySaleRow['indicator'],
#         'strategy_name': buySaleRow['strategy_name'],
#         'final_trade_date_time': buySaleRow['date']+" "+buySaleRow['time'],
#         'ticker': buySaleRow['ticker'],
#         'qty': 10,
#         'sell_signal_price': buySaleRow['sell_signal_price'],
#         'buy_signal_price': buySaleRow['buy_signal_price'],
#         'tradestatus': 'ORDER_PLACED'
#       }
#     tradeDf.loc[index2] = new_rowa
#   return tradeDf

# def updateTradeTable(tradeDF):
#   mycursor = mydb.cursor()
#   print(tradeDF.columns)
#   for idx, row in tradeDF.iterrows():
#     sqlQry = "insert into trade_data ('id', 'ticker', 'buy_signal_price', 'sell_signal_price', 'strategy_name', 'indicator', 'tradestatus', 'final_trade_date_time', 'qty') values ("+str(tradeDF['id'][idx])+",'"+str(tradeDF['ticker'][idx])+"',"+str(tradeDF['buy_signal_price'][idx])+","+str(tradeDF['sell_signal_price'][idx])+",'"+str(tradeDF['strategy_name'][idx])+"','"+str(tradeDF['indicator'][idx])+"','"+str(tradeDF['tradestatus'][idx])+"','"+str(tradeDF['final_trade_date_time'][idx])+"',"+str(tradeDF['qty'][idx])+")"
#     try:
#       mycursor.execute(sqlQry)
#       mydb.commit()
#     except:
#       print("Err in "+sqlQry)


# def updateStatusAfterTradeSuccess(oldStatus, newStatus):
#   mycursor = mydb.cursor()
#   sqlQry = "update trade_data set tradestatus = '"+newStatus+"' where tradestatus='"+oldStatus+"'"
#   try:
#     mycursor.execute(sqlQry)
#     mydb.commit()
#   except:
#     print("Err in "+sqlQry)

# def moveBuySaleRecordToArchive():
#   print("todo insert record to buy_sell_data_arch table")
#   print("write query to delete  record from buy_sell_data table if duplicate in buy_sell_data__arch table")

# def main():
#   print("Start trading thread change from TODOTRADING to SUCCESS")
#   toTradeBuySaleDF = getALLToTradeDataFromBuySaleTable("TODOTRADING")
#   print(" got all buy sale data with TO_TRADE : {} ".format(len(toTradeBuySaleDF)))
#   tradeDF = prepareTradeDF(toTradeBuySaleDF)
#   print(" convert to tradeDF : {} ".format(len(tradeDF)))
#   updateTradeTable(tradeDF)
#   print(" insert trade record into trade table : {} ".format(len(tradeDF)))
#   updateStatusAfterTradeSuccess("ORDER_PLACED", "TRADE_SUCCESS")
#   print(" updat status with trade success : {} ".format(len(tradeDF)))
#   moveBuySaleRecordToArchive(toTradeBuySaleDF)
#   print(" move buy sal record to archive table : {} ".format(len(toTradeBuySaleDF)))
#   print(" TODOTRADING to SUCCESS update FINISH")

# main()












# def calculateProfitLossIndicatorWise(newBuysaleDF):
#     # step1 group by script, indicator, datetime
#     profitLossDf = pd.DataFrame(columns=['id', 'indicator', 'strategy_name', 'final_trade_date_time', 'script', 'qty',
#             'sale_rate', 'purch_rate', 'brokerage', 'final_amount', 'trade_type' ])
#     for index2, buySaleRow in newBuysaleDF.iterrows():
#       new_row = {'id': time.time()*1000.0,
#         'indicator': buySaleRow['indicator'],
#         'strategy_name': buySaleRow['strategy_name'],
#         'final_trade_date_time': buySaleRow['final_trade_date_time'],
#         'script': buySaleRow['ticker'],
#         'qty': 10,
#         'sale_rate': float(buySaleRow['sell_signal_price'])*1.05,
#         'purch_rate': float(buySaleRow['buy_signal_price'])*1.05,
#         'brokerage': buySaleRow['sell_signal_price']*0.19,
#         'final_amount': float(buySaleRow['sell_signal_price'])-float(buySaleRow['buy_signal_price'])-float(buySaleRow['sell_signal_price'])*0.19,
#         'trade_type': 'EQ' }
#       profitLossDf.loc[index2] = new_row
#     return profitLossDf

# def updateProfitLossTable(profitLossDF):
#   mycursor = mydb.cursor()
#   for idx, row in profitLossDF.iterrows():
#     sqlQry = "insert into profit_loss_data values (NULL,'"+str(profitLossDF['indicator'][idx])+"','"+str(profitLossDF['strategy_name'][idx])+"','"+str(profitLossDF['final_trade_date_time'][idx])+"','"+str(profitLossDF['script'][idx])+"','"+str(profitLossDF['qty'][idx])+"','"+str(profitLossDF['sale_rate'][idx])+"','"+str(profitLossDF['purch_rate'][idx])+"','"+str(profitLossDF['brokerage'][idx])+"','"+str(profitLossDF['final_amount'][idx])+"','"+str(profitLossDF['trade_type'][idx])+"')"
#     try:
#       mycursor.execute(sqlQry)
#       mydb.commit()
#     except:
#       print("Err in "+sqlQry)

# def updateTradeDableAfterCalculation(newBuysaleDF, newStatus):
#   mycursor = mydb.cursor()
#   for idx, row in newBuysaleDF.iterrows():
#     sqlQry = "update trade_data set tradestatus='"+newStatus+"' where id="+str(newBuysaleDF['id'][idx])
#     try:
#       mycursor.execute(sqlQry)
#       mydb.commit()
#     except:
#       print("Err in "+sqlQry)

