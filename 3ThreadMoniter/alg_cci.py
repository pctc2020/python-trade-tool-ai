import pandas as pd 
import pandas_ta as ta
import numpy as np
import utils

#param: it require 2 parameters length and c
def cci_buy_sell(df,param):
    rapam_length = int(param[0])
    rapam_c = int(param[0])
    print("starting CCI analysis")
    cci_buy = []
    cci_sell = [] 
    position = False
    cci = ta.cci(df['high'],df['low'],df['close'], length=rapam_length, c=rapam_c)
    colName = "cci_{}_{}".format(rapam_length, rapam_c)
    df[colName] = cci 
    df = pd.concat([df, cci], axis=1).reindex(df.index)
    #df.rename(columns = {"CCI_14_0.015":"CCI_line"},inplace=True)

    upper_band = 150
    Lower_band = (-150)
    for i in range(len(df)):
        if df[colName][i] < Lower_band :
            #if position == False:
            if not position:
                cci_buy.append(df['close'][i])
                cci_sell.append(np.nan)
                position = True
            else:
                cci_buy.append(np.nan)
                cci_sell.append(np.nan)
        elif df[colName][i] > upper_band:
           # if position == True:
            if position:
                cci_buy.append(np.nan)
                cci_sell.append(df['close'][i])
                position = True
            else:
                cci_buy.append(np.nan)
                cci_sell.append(np.nan)
        else:
            cci_buy.append(np.nan)
            cci_sell.append(np.nan)
            
            
    print("CCI Analysis completed")
    df = df.drop(labels=[colName], axis=1)
    df['buy_signal_price'] = cci_buy
    df['sell_signal_price'] = cci_sell
    df["strategy_name"] = "cci_{}_{}".format(rapam_length, rapam_c)
    df['indicator'] = "cci"
    df['tradestatus'] = "Completed"
    #df['buy_signal_price'], df['sell_signal_price'] = pd.Series([cci_buy, cci_sell])
    print("TODO column rename")
    utils.update_data_table(df)
    return (df)


