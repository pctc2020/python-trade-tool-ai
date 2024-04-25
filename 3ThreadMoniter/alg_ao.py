import pandas as pd
import numpy as np
import pandas_ta as ta
import utils

#param: it require 1 parameters length and def
def ao_buy_sell(df, risk, mydb):
    # Baaki function ka code...
    param_fast =1
    param_slow = 1
    ao_buy = []
    ao_sell = []
    position = False
    #ao= ta.ao(df['high'], df['low'], df['close'], sam=risk1_sma)
    ao = ta.ao(df['high'],df['low'], fast=param_fast, slow=param_slow)
    df = pd.concat([df,ao], axis=1).reindex(df.index)
    colName = 'AO_'+str(param_fast)+'_'+str(param_slow)
    for i in range(len(df)):
        if df[colName][i]> 0:
            if position == False:
                ao_buy.append(df['close'][i])
                ao_sell.append(np.nan)
                position = True
            else:
                ao_buy.append(np.nan)
                ao_sell.append(np.nan)
        elif df[colName][i]< 0:
            if position == True:
                ao_buy.append(np.nan)
                ao_sell.append(df['close'][i])
                position = False
            else:
                ao_buy.append(np.nan)
                ao_sell.append(np.nan)
        else:
            ao_buy.append(np.nan)
            ao_sell.append(np.nan)
        
    print("AO Analysis completed")
    data = df.drop(labels=[colName], axis=1)
    data['buy_signal_price'],data['sell_signal_price'] = pd.Series([ao_buy, ao_sell]) 
    data["strategy_name"]=colName
    data['indicator'] = "ao"
    data['tradestatus'] = "Completed"
    utils.update_data_table(data)
    # print("Indicators123: ", df.columns)
    return data


