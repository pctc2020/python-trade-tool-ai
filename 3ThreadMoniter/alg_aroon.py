import pandas as pd
import numpy as np
import pandas_ta as ta
import utils
# import executor

#param: it require 2 parameters fast and slow
def aroon_buy_sell(df, param):
    param_fast = int(param[0])
    param_slow = int(param[0])
    aroon_buy = []
    aroon_sell = []
    position = False
    aroon = ta.aroon(df['high'], df['low'], fast=param_fast, slow=param_slow)
    df = pd.concat([df,aroon], axis=1).reindex(df.index)
    for i in range(len(df)):
        if df['AROONU_14'][i] >= 70 and df['AROOND_14'][i] <= 30:
            if position == False:
                aroon_buy.append(df['close'][i])
                aroon_sell.append(np.nan)
                position = True
            else:
                aroon_buy.append(np.nan)
                aroon_sell.append(np.nan)
        elif df['AROONU_14'][i] <= 30 and df['AROOND_14'][i] >= 70:
            if position == True:
                aroon_buy.append(np.nan)
                aroon_sell.append(df['close'][i])
            else:
                aroon_buy.append(np.nan)
                aroon_sell.append(np.nan)
        else:
            aroon_buy.append(np.nan)
            aroon_sell.append(np.nan)
    print("AROON Analysis completed")
    df['buy_signal_price'],df['sell_signal_price'] = pd.Series([aroon_buy,aroon_sell]) 
    df["strategy_name"]="aroon_{}".format(param_fast)+"_{}".format(param_slow)
    df['indicator'] = "aroon"
    df['tradestatus'] = "Completed"
    print("TODO= column rename")
    utils.update_data_table(df)
    return df
