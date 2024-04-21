import pandas as pd
import numpy as np
import pandas_ta as ta
import utils
# import executor

#param: it require 2 parameters length, multiplier
def st_buy_sell(df,params):
    param1_k = int(params[0])
    param2_d= float(params[1])
    #d_period = float(d_period[0])
    st_buy = []
    st_sell = []
    position = False
    #st = ta.supertrend(df['high'],df['low'],df['close'] ,param=param1_k,param=param2_d)
    st= ta.supertrend(df['high'], df['low'], df['close'], length=param1_k, multiplier=param2_d)

     # Construct the Supertrend column name
    supertrend_column = f'SUPERT_{param1_k}_{param2_d}'
    #df = pd.concat([df,st], axis=1).reindex(df.index)
    df = pd.concat([df, st], axis=1).reindex(df.index)
    #risk = "BBL_10_1.5"
    #risk = "BBL_"+str(param1_length)+"_2.0"
    #risk = "BBL_"+str(k_period)+"_"+str(d_period)
   
    for i in range(len(df)):
        #if df['SUPERT_7_3.0'][i] < df['close'][i] :
        if df[supertrend_column][i] < df['close'][i]:
            if position == False:
                st_buy.append(df['close'][i])
                st_sell.append(np.nan)
                position = True
            else:
                st_buy.append(np.nan)
                st_sell.append(np.nan)
        elif df[supertrend_column][i] > df['close'][i]:
    
            if position == True:
                st_buy.append(np.nan)
                st_sell.append(df['close'][i])
                position = False
            else:
                st_buy.append(np.nan)
                st_sell.append(np.nan)
        else:
            st_buy.append(np.nan)
            st_sell.append(np.nan)
    print("ST Analysis completed")
    df['buy_signal_price'],df['sell_signal_price'] = pd.Series([st_buy,st_sell]) 
    df["strategy_name"]="st_{}_{}".format(param1_k, param2_d)
    df['indicator'] = "st"
    df['tradestatus'] = "Completed"
    print("TODO column rename") 
    utils.update_data_table(df)
    return (df)


