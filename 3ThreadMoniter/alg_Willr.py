import pandas as pd
import numpy as np
import pandas_ta as ta

# Define the willr_buy_sell function
# Requires only 1 parameter: period 
def willr_buy_sell(df, param):
    print("Starting willr analysis")
    param1_length = int(param[0])                         
    # Corrected the parameter indexing for the period    
    willr = ta.willr(df['high'], df['low'], df['close'], length=param1_length)
    willr_buy = []
    willr_sell = []
    position = False 

    df = pd.concat([df, willr], axis=1).reindex(df.index)
    print( df.columns)
    colName = "WILLR_"+str(param1_length)
    #print("risk = ", risk)
    
    for i in range(len(df)):
        if df[colName][i] < -80:  # Updated to use the correct column name "WILLR"
            if not position:
                willr_buy.append(df['close'][i])
                willr_sell.append(np.nan)
                position = True
            else:
                willr_buy.append(np.nan)
                willr_sell.append(np.nan)
        elif df[colName][i] > -20:  # Updated to use the correct column name "WILLR"
            if position:
                willr_buy.append(np.nan)
                willr_sell.append(df['close'][i])
                position = False
            else:
                willr_buy.append(np.nan)
                willr_sell.append(np.nan)
        else:
            willr_buy.append(np.nan)
            willr_sell.append(np.nan)

    df['buy_signal_price'], df['sell_signal_price'] = pd.Series([willr_buy, willr_sell])
    df["strategy_name"] = colName  # Updated the strategy_name format
    df['indicator'] = "willr"
    
    df = df.drop(columns=colName)  # Corrected the column name to drop
    df.rename(columns={"Date": "Trade_Date", "time": "Trade_time"}, inplace=True)    
    print("TODO column rename") 
    return (df)
