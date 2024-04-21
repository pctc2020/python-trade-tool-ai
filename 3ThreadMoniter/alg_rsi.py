import pandas_ta as ta
import pandas as pd
import numpy as np
import utils 
# import executor

#param: it require 1 parameters length 
def rsi_buy_sell(df,param):
    param_length = int(param[0])
    print("Starting RSI analysis")
    rsi_buy = []
    rsi_sell = []
    position = False
    validationRange  = "RSI_"+str(param_length)
    print("validationRange=", validationRange)
    rsi = ta.rsi(df['close'], length=param_length)
    df = pd.concat([df, rsi], axis=1).reindex(df.index)
    for i in range (len(df)):
        if df[validationRange][i] < 30:
            if position == False:
                rsi_buy.append(df['close'][i])
                rsi_sell.append(np.nan)
                position = True
            else:
                rsi_buy.append(np.nan)
                rsi_sell.append(np.nan)
        elif df[validationRange][i] > 70:
            if position == True:
                rsi_buy.append(np.nan)
                rsi_sell.append(df['close'][i])
                position = False
            else:
                rsi_buy.append(np.nan)
                rsi_sell.append(np.nan)
        else:
            rsi_buy.append(np.nan)
            rsi_sell.append(np.nan)
    print("RSI Analysis completed")
    df['buy_signal_price'], df['sell_signal_price'] = pd.Series([rsi_buy, rsi_sell])
    df["strategy_name"]="rsi_{}".format(param_length)
    df['indicator'] = "rsi"
    df['tradestatus'] = "Completed"
    print("TODO column rename") 
    utils.update_data_table(df)
    return (df)

        

