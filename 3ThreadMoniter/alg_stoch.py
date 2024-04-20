import pandas as pd
import pandas_ta as ta
import numpy as np
# import executor

#param: it require 3 parameters fastk, slowk, slowd
def stoch_buy_sell(df,risk):
    risk1_fastk = int(risk[0])
    risk2_slowk = int(risk[1])
    risk3_slowd = float(risk[2])

    print("starting stoch analysis")
    stoch_buy = []
    stoch_sell = []
    position = False
    stoch = ta.stoch(df['high'], df['low'], df['close'], k=risk1_fastk, d=risk2_slowk, smooth_k=risk3_slowd)
    df = pd.concat([df, stoch], axis=1).reindex(df.index)
    colNameSfx = "_{}_{}_{}".format(risk1_fastk, risk2_slowk, risk3_slowd)
    for i in range (len(df)):
        if df['STOCHk'+colNameSfx][i] < 20 and df['STOCHd'+colNameSfx][i] < 20 and df['STOCHk'+colNameSfx][i] < df['STOCHd'+colNameSfx][i] :
            if position == False:
                stoch_buy.append(df['close'][i])
                stoch_sell.append(np.nan)
                position =True
            else:
                stoch_buy.append(np.nan)
                stoch_sell.append(np.nan)
        elif df['STOCHk'+colNameSfx][i] > 80 and df['STOCHd'+colNameSfx][i] > 80 and df['STOCHk'+colNameSfx][i] > df['STOCHd'+colNameSfx][i] :
            if position == True:
                stoch_buy.append(np.nan)
                stoch_sell.append(df['close'][i])
                position = False
            else:
                stoch_buy.append(np.nan)
                stoch_sell.append(np.nan)
        else:
            stoch_buy.append(np.nan)
            stoch_sell.append(np.nan)
    print("stoch Analysis completed")
    df['buy_signal_price'],df['sell_signal_price'] = pd.Series([stoch_buy,stoch_sell]) 
    df["strategy_name"]="stoch_{}".format(risk)
    df['indicator'] = "stoch"
    print("TODO column rename") 
    return (df)