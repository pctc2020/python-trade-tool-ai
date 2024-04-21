import pandas as pd
import pandas_ta as ta
import numpy as np
import utils
# import executor

#param: it require 3 parameters params : short, long, singal
# Define the macd_buy_sell function
def macd_buy_sell(df, params):
    param1_short = int(params[0])
    param2_long = int(params[1])
    param3_signal = int(params[2])  # Corrected the parameter index
    print("Starting MACD analysis")
    
    MACD_Buy = []
    MACD_Sell = []
    position = False
    
    # Calculate MACD
    macd = ta.macd(df['close'], fast=param1_short, slow=param2_long, signal=param3_signal)
    
    # The MACD function generates columns with dynamic names, so let's get those column names
    macd_columns = list(macd.columns)
    
    df = pd.concat([df, macd], axis=1).reindex(df.index)
    
    for i in range(0, len(df)):
        if df[macd_columns[0]][i] > df[macd_columns[2]][i]:
            MACD_Sell.append(np.nan)
            if position == False:
                MACD_Buy.append(df['close'][i])
                position=True
            else:
                MACD_Buy.append(np.nan)
        elif df[macd_columns[0]][i] < df[macd_columns[2]][i]:
            MACD_Buy.append(np.nan)
            if position == True:
                MACD_Sell.append(df['close'][i])
                position=False
            else:
                MACD_Sell.append(np.nan)
        elif position == True and df['close'][i] < MACD_Buy[-1] * (1 - params[1]):
            MACD_Sell.append(df["close"][i])
            MACD_Buy.append(np.nan)
            position = False
        elif position == True and df['close'][i] < df['close'][i - 1] * (1 - params[1]):
            MACD_Sell.append(df["close"][i])
            MACD_Buy.append(np.nan)
            position = False
        else:
            MACD_Buy.append(np.nan)
            MACD_Sell.append(np.nan)
    
    print("MACD Analysis completed")
    df['buy_signal_price'], df['sell_signal_price'] = pd.Series([MACD_Buy, MACD_Sell])
    df["strategy_name"]="macd_{}_{}_{}".format(param1_short, param2_long, param3_signal)
    df['indicator'] = "macd"
    df['tradestatus'] = "Completed"
    print("TODO column rename") 
    utils.update_data_table(df)
    return (df)
