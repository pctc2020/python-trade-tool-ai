import time
import unittest
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from numpy.random import randint
# mysql connection 
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
# -------------------------shivam-----------------------------------------------------

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
    db_cursor.execute("select * from trade_data where tradestatus='"+tradeStatus+"' and ticker='"+ticeker+"' and indicator='"+indicator+"'")
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
            print("Table Already Exist")
        else:
            create_query = """CREATE TABLE profit_loss_data (
                                id BIGINT,
                                indicator VARCHAR(255),
                                strategy_name VARCHAR(255),
                                final_trade_date_time TIMESTAMP,
                                script VARCHAR(255),
                                qty int,
                                sale_price VARCHAR(255),
                                purchase_price VARCHAR(255),
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
# -------------------------------------------------------------------------------

def calculateProfitLossIndicatorWise(tradeDF):
    # step1 group by script, indicator, datetime
    profitLossDf = pd.DataFrame(columns=['id', 'indicator', 'strategy_name', 'final_trade_date_time', 'script', 'qty',
            'sale_rate', 'purch_rate', 'brokerage', 'final_amount', 'trade_type' ])
    for index2, buySaleRow in tradeDF.iterrows():
      new_row = {'id': buySaleRow['id'],
        'indicator': buySaleRow['indicator'],
        'strategy_name': buySaleRow['strategy_name'],
        'final_trade_date_time': buySaleRow['final_trade_date_time'],
        'script': buySaleRow['ticker'],
        'qty': buySaleRow['qty'],
        # buy_price = row[0] * 1.0105  # Adding 1.05% to buy_signal_price
#         sell_price = row[1] * 0.9895  # Subtracting 1.05% from sell_signal_price
        'sale_rate': float(buySaleRow['sell_signal_price'])*1.0105, # Adding 1.05% to buy_signal_price
        'purch_rate': float(buySaleRow['buy_signal_price'])*0.9895, # Subtracting 1.05% from sell_signal_price
        'brokerage': buySaleRow['sell_signal_price']*0.19,
        'final_amount': float(buySaleRow['sell_signal_price'])-float(buySaleRow['buy_signal_price'])-float(buySaleRow['sell_signal_price'])*0.19,
        'trade_type': 'EQ' }
      profitLossDf.loc[index2] = new_row
    return profitLossDf

def updateProfitLossTable(profitLossDF):
  mycursor = mydb.cursor()
  for idx, row in profitLossDF.iterrows():
    sqlQry = "insert into profit_loss_data values (NULL,'"+str(profitLossDF['indicator'][idx])+"','"+str(profitLossDF['strategy_name'][idx])+"','"+str(profitLossDF['final_trade_date_time'][idx])+"','"+str(profitLossDF['script'][idx])+"','"+str(profitLossDF['qty'][idx])+"','"+str(profitLossDF['sale_rate'][idx])+"','"+str(profitLossDF['purch_rate'][idx])+"','"+str(profitLossDF['brokerage'][idx])+"','"+str(profitLossDF['final_amount'][idx])+"','"+str(profitLossDF['trade_type'][idx])+"')"
    try:
      mycursor.execute(sqlQry)
      mydb.commit()
    except:
      print("Err in "+sqlQry)

def updateTradeDableAfterCalculation(tradeDF, newStatus):
  mycursor = mydb.cursor()
  for idx, row in tradeDF.iterrows():
    sqlQry = "update trade_data set tradestatus='"+newStatus+"' where id="+str(tradeDF['id'][idx])
    try:
      mycursor.execute(sqlQry)
      mydb.commit()
    except:
      print("Err in "+sqlQry)

def main():
  tradeDF = getTradeSuccessData("HDFC", "SME" , "TRADE_SUCCESS")  #not started
  print("got all TRADE_SUCCESS record into tradeDF : {} ".format(len(tradeDF))) #not started
  profitLossDF = calculateProfitLossIndicatorWise(tradeDF)
  print("calculated profitLossDF : {} ".format(len(profitLossDF)))
  updateProfitLossTable(profitLossDF)
  print("update profit loss in database : {} ".format(len(profitLossDF)))
  updateTradeDableAfterCalculation(tradeDF, "PL_CALCULATED")
  print("update PL_CALCULATED status in trade table : {} ".format(len(tradeDF)))



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



