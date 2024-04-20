import pandas as pd
import numpy as np
import pandas_ta as ta
# import executor

#param: it require 4 parameters 
# length (int): WMA period. Default: 10 (5-40)
# fast (int): Fast ROC period. Default: 11 (5-45)
# slow (int): Slow ROC period. Default: 14 (5-50)
# offset (int): How many periods to offset the result. Default: 0 (0-10)
def coppock_buy_sell(df,params):
    param1_short = int(params[0])
    param1_fast = int(params[1])
    param1_slow = int(params[2])
    param1_offset = int(params[3])
    coppock_buy = []
    coppock_sell = []
    position = False

    coppock = ta.coppock(df['close'], length=param1_short, fast=param1_fast, slow=param1_slow, offset=param1_offset)
    df = pd.concat([df, coppock], axis=1).reindex(df.index)

    for i in range(len(df)):
        if df['COPC_'+str(param1_short)+'_'+str(param1_slow)+'_'+str(param1_offset)][i] > 0:
            if position == False:
                coppock_buy.append(df['close'][i])
                coppock_sell.append(np.nan)
                position == True
            else:
                coppock_buy.append(np.nan)
                coppock_sell.append(np.nan)
        elif df['COPC_'+str(param1_short)+'_'+str(param1_slow)+'_'+str(param1_offset)][i] < 0:
            if position == True:
                coppock_buy.append(np.nan)
                coppock_sell.append(df['close'][i])
                position = False
            else:
                coppock_buy.append(np.nan)
                coppock_sell.append(np.nan)
        else:
            coppock_buy.append(np.nan)
            coppock_sell.append(np.nan)
    print("COPPOCK Analysis completed")
    df['buy_signal_price'],df['sell_signal_price'] = pd.Series([coppock_buy,coppock_sell]) 
    df["strategy_name"]="coppock_{}".format(param1_short)
    df['indicator'] = "coppock"
    print("TODO column rename") 
    return (df)