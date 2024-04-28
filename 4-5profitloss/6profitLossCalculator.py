import time
import unittest
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from numpy.random import randint

def db_connection():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="Root",
        database="stockmarket"
    )
    return mydb


def get_all_to_trade_data_from_buy_sale_table(mydb):
    db_cursor=mydb.cursor()
    db_cursor.execute("SELECT * FROM trade_data WHERE tradestatus='TRADE_SUCCESS'")
    data = db_cursor.fetchall()
    # # Get column names from cursor description
    columns = [i[0] for i in db_cursor.description]
    # # Create DataFrame from fetched MySQL data and provide column labels
    df = pd.DataFrame(data, columns=columns)
    # print(df)
    return df

#create table code...
def create_profit_loss_data_table(mydb):
    try:
        cursor = mydb.cursor()
        cursor.execute("SHOW TABLES LIKE 'profit_loss_data'")
        result = cursor.fetchone()
        if result:
            print("profit_loss_data Table Already Exist")
        else:
            create_query = """CREATE TABLE profit_loss_data (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                indicator VARCHAR(255),
                                strategy_name VARCHAR(255),
                                final_trade_date_time TIMESTAMP,
                                script VARCHAR(255),
                                qty int,
                                sale_rate FLOAT,
                                purchase_rate FLOAT,
                                brokerage FLOAT,
                                final_amount FLOAT,
                                trade_type VARCHAR(255)
                            )"""
            cursor.execute(create_query)
            print("profit_loss_data Table Created Successfully")

        mydb.commit()
        cursor.close()    

    except Exception as e:
        print("Error:", e)

def insert_data_to_profit_loss_data_table(df, mydb):    
    try:
        cursor = mydb.cursor()
        create_profit_loss_data_table(mydb)
        for index, row in df.iterrows():
            script = row['ticker']
            sale_rate = float(row['sell_signal_price'])*0.9895# Subtracting 1.05% from sell_signal_price
            purchase_rate = float(row['buy_signal_price'])*1.0105  # Adding 1.05% to buy_signal_price
            brokerage = float(row['sell_signal_price'])*0.21
            final_amount = sale_rate - purchase_rate
            trade_type = 'EQ'
            # SQL query to insert data into the table
            query = """INSERT INTO profit_loss_data 
                       (indicator, strategy_name, final_trade_date_time, script, qty, sale_rate, purchase_rate, brokerage, final_amount, trade_type) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""  
            values = (row['indicator'], row['strategy_name'], row['final_trade_date_time'], script, row['qty'], sale_rate, purchase_rate, brokerage, final_amount, trade_type)
            cursor.execute(query, values)
            mydb.commit()
            print("Data inserted successfully!")
        cursor.close()
    except Exception as e:
        print("Error:", e)

# data fetch profit_loss_table
def get_all_profit_loss_table_data(mydb):
    db_cursor=mydb.cursor()
    db_cursor.execute("SELECT * FROM profit_loss_data")
    data = db_cursor.fetchall()
    # # Get column names from cursor description
    columns = [i[0] for i in db_cursor.description]
    # # Create DataFrame from fetched MySQL data and provide column labels
    df = pd.DataFrame(data, columns=columns)
    # print(df)
    return df

#create investment wise table code...
def create_investment_table_data_(mydb):
    try:
        cursor = mydb.cursor()
        cursor.execute("SHOW TABLES LIKE 'investment_table_data'")
        result = cursor.fetchone()
        if result:
            print("Table Already Exist")
        else:
            create_query = """CREATE TABLE investment_table_data (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                indicator VARCHAR(255),
                                strategy_name VARCHAR(255),
                                final_trade_date_time TIMESTAMP,
                                script VARCHAR(255),
                                qty int,
                                sale_rate FLOAT,
                                purchase_rate FLOAT,
                                brokerage FLOAT,
                                final_amount FLOAT,
                                trade_type VARCHAR(255)
                            )"""
            cursor.execute(create_query)
            print("Table Created Successfully")

        mydb.commit()
        cursor.close()    

    except Exception as e:
        print("Error:", e)

def insert_data_to_investment_table_data_(df, mydb):    
    try:
        cursor = mydb.cursor()
        create_investment_table_data_(mydb) 
        for index, row in df.iterrows():
          if row['purchase_rate'] != 0:
              qty = 10000/row['purchase_rate']
              # SQL query to insert data into the table
              query = """INSERT INTO investment_table_data 
                        (indicator, strategy_name, final_trade_date_time, script, qty, sale_rate, purchase_rate, brokerage, final_amount, trade_type) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""  
              values = (row['indicator'], row['strategy_name'], row['final_trade_date_time'], row['script'], qty, row['sale_rate'], row['purchase_rate'], row['brokerage'], row['final_amount'], row['trade_type'])
              cursor.execute(query, values)
              mydb.commit()
              print("Data inserted successfully!")
          else:
              print("Error: Cannot divide by zero for purchase_rate =", row['purchase_rate'])
        cursor.close()
        cursor.close()
    except Exception as e:
        print("Error:", e)

def update_Trade_table_After_Calculation(tradeDF, newStatus, mydb):
  mycursor = mydb.cursor()
  for idx, row in tradeDF.iterrows():
    sqlQry = "update trade_data set tradestatus='"+newStatus+"' where id="+str(tradeDF['id'][idx])
    try:
      mycursor.execute(sqlQry)
      mydb.commit()
    except Exception as e:
      print("Error:", e)



def main():
    try:
        mydb = db_connection()
        tradeDF = get_all_to_trade_data_from_buy_sale_table(mydb)
        print("got all TRADE_SUCCESS record into tradeDF : {} ".format(len(tradeDF)))
        insert_data_to_profit_loss_data_table(tradeDF, mydb)
        print("updated profit_loss_data_table : {} ".format(len(tradeDF)))
        Profit_Loss_table = get_all_profit_loss_table_data(mydb)
        print("got all Profit_loss record : {} ".format(len(Profit_Loss_table)))
        insert_data_to_investment_table_data_(Profit_Loss_table, mydb)
        print("updated investment_table_data: {} ".format(len(Profit_Loss_table)))
        # update_Trade_table_After_Calculation(tradeDF, "PL_Investment_Calculated", mydb)
        # print("update trade status in trade table : {} ".format(len(tradeDF)))
    except Exception as e:
      print("Error:",e)
    finally:
      if mydb:
          mydb.close()

if __name__=="__main__":
    main()


# -------------------------------------------------------------------------------
# # mysql connection 
# mydb = mysql.connector.connect(
#   host="localhost",
#   user="root",
#   password="Root",
#   database="stockmarket"
# )

# def getTradeSuccessData(ticeker, indicator, tradeStatus):
#     selQry = "select * from trade_data where tradestatus='"+tradeStatus+"' and ticker='"+ticeker+"' and indicator='"+indicator+"'"
#     dataframe =  pd.read_sql(selQry, mydb)
#     return dataframe


# def calculateProfitLossIndicatorWise(tradeDF):
#     # step1 group by script, indicator, datetime
#     profitLossDf = pd.DataFrame(columns=['id', 'indicator', 'strategy_name', 'final_trade_date_time', 'script', 'qty',
#             'sale_rate', 'purch_rate', 'brokerage', 'final_amount', 'trade_type' ])
#     for index2, buySaleRow in tradeDF.iterrows():
#       new_row = {'id': buySaleRow['id'],
#         'indicator': buySaleRow['indicator'],
#         'strategy_name': buySaleRow['strategy_name'],
#         'final_trade_date_time': buySaleRow['final_trade_date_time'],
#         'script': buySaleRow['ticker'],
#         'qty': buySaleRow['qty'],
#         # buy_price = row[0] * 1.0105  # Adding 1.05% to buy_signal_price
# #         sell_price = row[1] * 0.9895  # Subtracting 1.05% from sell_signal_price
#         'sale_rate': float(buySaleRow['sell_signal_price'])*0.9895, # Adding 1.05% to buy_signal_price
#         'purch_rate': float(buySaleRow['buy_signal_price'])*1.0105, # Subtracting 1.05% from sell_signal_price
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

# def updateTradeDableAfterCalculation(tradeDF, newStatus):
#   mycursor = mydb.cursor()
#   for idx, row in tradeDF.iterrows():
#     sqlQry = "update trade_data set tradestatus='"+newStatus+"' where id="+str(tradeDF['id'][idx])
#     try:
#       mycursor.execute(sqlQry)
#       mydb.commit()
#     except:
#       print("Err in "+sqlQry)

# def main():
#   tradeDF = getTradeSuccessData("HDFC", "SME" , "TRADE_SUCCESS")  #not started
#   print("got all TRADE_SUCCESS record into tradeDF : {} ".format(len(tradeDF))) #not started
#   profitLossDF = calculateProfitLossIndicatorWise(tradeDF)
#   print("calculated profitLossDF : {} ".format(len(profitLossDF)))
#   updateProfitLossTable(profitLossDF)
#   print("update profit loss in database : {} ".format(len(profitLossDF)))
#   updateTradeDableAfterCalculation(tradeDF, "PL_CALCULATED")
#   print("update PL_CALCULATED status in trade table : {} ".format(len(tradeDF)))



# main()

# -----------------------------------------------------------------------------------------
# import mysql.connector

# def Actual_buy_sell_price(mydb):
#     try:
#         cursor = mydb.cursor()
#         cursor.execute("ALTER TABLE buy_sell_data ADD COLUMN buy_price DECIMAL(10, 2)")
#         cursor.execute("ALTER TABLE buy_sell_data ADD COLUMN sell_price DECIMAL(10, 2)")
#         cursor.execute("ALTER TABLE buy_sell_data ADD COLUMN Quantity DECIMAL(10, 2)")
#         cursor.execute("ALTER TABLE buy_sell_data ADD COLUMN brockrage_buy_price DECIMAL(10, 2)")
#         cursor.execute("ALTER TABLE buy_sell_data ADD COLUMN brockrage_sell_price DECIMAL(10, 2)")
#         cursor.execute("SELECT buy_signal_price, sell_signal_price FROM buy_sell_data")
#         rows = cursor.fetchall()
#         for row in rows:
#             buy_price = row[0] * 1.0105  # Adding 1.05% to buy_signal_price
#             sell_price = row[1] * 0.9895  # Subtracting 1.05% from sell_signal_price
#             Quantity = 10
#             brockrage_buy_price = buy_price - row[0] 
#             brockrage_sell_price = row[1] - sell_price
#             cursor.execute("INSERT INTO buy_sell_data (buy_price, sell_price, Quantity, brockrage_buy_price, brockrage_sell_price) VALUES (%s, %s, %s, %s, %s)", (buy_price, sell_price, Quantity, brockrage_buy_price, brockrage_sell_price))
# # formula name , indicator name, trade_date_time
# # brokerage_buy_price,         
#         mydb.commit()
#         cursor.close()

#         print("Actual buy and sell prices updated successfully!")

#     except Exception as e:
#         print("Error:", e)

# def investment_wise(mydb):
#   try:
#       cursor = mydb.cursor()
#       create_investment_table(mydb)
#       cursor.execute("SELECT script, buy_price, sell_price, indicator, brockrage_buy_price, brockrage_sell_price FROM buy_sell_data")
#       rows = cursor.fetchall()
#       Quantities = []
#       for row in rows:
#         script = row[0]
#         actual_buy_price = row[1]
#         actual_sell_price = row[2]
#         Quantity = 10000/actual_buy_price
#         indicator = row[3]
#         brockrage_buy_price = row[4]
#         brockrage_sell_price = row[5]
#         Quantities.append((script, actual_buy_price, actual_sell_price, Quantity, indicator, brockrage_buy_price, brockrage_sell_price))
      
#         insert_query = "INSERT INTO investment_table (script, actual_buy_price, actual_sell_price, quantity, indicator, brockrage_buy_price, broack_sell_price) VALUES (%s, %s, %s, %s, %s, %s, %s)"
#         cursor.executemany(insert_query, Quantities)

#       mydb.commit()
#       cursor.close()
#   except Exception as e:
#       print("Error:", e)


# def create_investment_table(mydb):
#     try:
#         cursor = mydb.cursor()
#         cursor.execute("SHOW TABLES LIKE 'investment_table'")
#         result = cursor.fetchone()
#         if result:
#             print("Table Already Exist")
#         else:
#             create_query = """CREATE TABLE investment_table (
#                                 id INT AUTO_INCREMENT PRIMARY KEY,
#                                 script VARCHAR(255),
#                                 actual_buy_price DECIMAL(10, 2),
#                                 actual_sell_price DECIMAL(10, 2),
#                                 Quantity DECIMAL(10, 2),
#                                 indicator VARCHAR(255),
#                                 brockrage_buy_price DECIMAL(10, 2),
#                                 brockrage_sell_price DECIMAL(10, 2)
#                             )"""
#             cursor.execute(create_query)
#             print("Table Created Successfully")

#         mydb.commit()
#         cursor.close()    

#     except Exception as e:
#         print("Error:", e)

# # Example usage:
# # create_investment_table(mydb)
# def main():
#   try:
#     Actual_buy_sell_price(mydb)
#     investment_wise(mydb)
#   except Exception as e:
#     print("error:", e)

# main()



