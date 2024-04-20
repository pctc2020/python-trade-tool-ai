import time
import unittest
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from numpy.random import randint
# mysql connection 
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="admin",
  database="shop_portal"
)

def getTradeSuccessData(ticeker, indicator, tradeStatus):
    selQry = "select * from trade_data where tradestatus='"+tradeStatus+"' and ticker='"+ticeker+"' and indicator='"+indicator+"'"
    dataframe =  pd.read_sql(selQry, mydb)
    return dataframe


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
        'sale_rate': float(buySaleRow['sell_signal_price'])*1.05,
        'purch_rate': float(buySaleRow['buy_signal_price'])*1.05,
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
  # tradeDF = getTradeSuccessData("HDFC", "SME" , "TRADE_SUCCESS")
  # print("got all TRADE_SUCCESS record into tradeDF : {} ".format(len(tradeDF)))
  # profitLossDF = calculateProfitLossIndicatorWise(tradeDF)
  print("calculated profitLossDF : {} ".format(len(profitLossDF)))
  updateProfitLossTable(profitLossDF)
  print("update profit loss in database : {} ".format(len(profitLossDF)))
  updateTradeDableAfterCalculation(tradeDF, "PL_CALCULATED")
  print("update PL_CALCULATED status in trade table : {} ".format(len(tradeDF)))
  Actual_buy_sell_price()

main()

# -----------------------------------------------------------------------------------------
import mysql.connector

def Actual_buy_sell_price():
    try:
        
        # Create a cursor object
        cursor = mydb.cursor()

        # Fetching data from buy_sell_data table
        cursor.execute("SELECT buy_signal_price, sell_signal_price FROM buy_sell_data")

        # Fetch all rows
        rows = cursor.fetchall()

        # Iterate through rows and calculate actual buy and sell prices
        for row in rows:
            buy_price = row[0] * 1.0105  # Adding 1.05% to buy_signal_price
            sell_price = row[1] * 0.9895  # Subtracting 1.05% from sell_signal_price
            Quantity = 10

            # Inserting calculated values into the table
            cursor.execute("INSERT INTO buy_sell_data (actual_buy_price, actual_sell_price, Quantity) VALUES (%s, %s, %s)", (buy_price, sell_price, Quantity))
        
        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        print("Actual buy and sell prices updated successfully!")

    except Exception as e:
        print("Error:", e)

def investment_wise(mydb):
  try:
      cursor = mydb.cursor()
      cursor.execute("SELECT actual_buy_price, actual_sell_price FROM buy_sell_data")
      rows = cursor.fetchall()
      Quantities = []
      for row in rows:
        actual_buy_price = row[0]
        actual_sell_price = row[1]
        Quantity = 10000/actual_buy_price
        Quantities.append((Quantity, actual_buy_price, actual_sell_price))
      
      insert_query = "INSERT INTO investment_table (quantity, buy_price, sell_price) VALUES (%s, %s, %s)"
      cursor.executemany(insert_query, Quantities)

      conn.commit()
      cur.close()
  except Exception as e:
      print("Error:", e)







