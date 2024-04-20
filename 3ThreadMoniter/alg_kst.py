import pandas as pd
import numpy as np
import pandas_ta as ta
# import executor

#param: it require 3 parameters roc1*1000000+roc2*10000*roc3*100 and sma1*1000000+sma2*10000+sma3*100, signal\
# if u pass param = [100101102103, 104105106107, 25] then it come as sma1,2,3= 25 107 106 105 4 AND roc1,2,3 = 103 102 101 100
def kst_buy_sell(df,param):
    param_roc1 = int( (param[0])%1000)
    param_roc2 = int( (param[0]/1000)%1000)
    param_roc3 = int( (param[0]/1000000)%1000)
    param_roc4 = int( (param[0]/1000000000)%1000)
    param_sma1 = int( param[1]%1000 )
    param_sma2 = int( (param[1]/1000)%1000)
    param_sma3 = int( (param[1]/1000000)%1000)
    param_sma4 = int( (param[1]/1000000000)%1000)
    param_signal = int(param[2])
# 11_15_20_30_11_10_10_15
    print("KST_"+str(param_roc1)+"_"+str(param_roc2)+"_"+str(param_roc3)+"_"+str(param_roc4)+"_"+str(param_sma1)+"_"+str(param_sma2)+"_"+str(param_sma3)+"_"+str(param_sma4))
    print("starting kst analysis")
    kst_buy = []
    kst_sell = []
    position = False
    kst = ta.kst(df['close'], roc1=param_roc1, roc2=param_roc2, roc3=param_roc3, roc4=param_roc4, sma1=param_sma1, sma2=param_sma2, sma3=param_sma3, sma4=param_sma4, signal=param_signal)
    df = pd.concat([df, kst], axis=1).reindex(df.index)
    df.rename(columns = {"KST_"+str(param_roc1)+"_"+str(param_roc2)+"_"+str(param_roc3)+"_"+str(param_roc4)+"_"+str(param_sma1)+"_"+str(param_sma2)+"_"+str(param_sma3)+"_"+str(param_sma4):"KST_line","KSTs_"+str(param_signal):"signal_line"},inplace=True)
    # print(df.columns)
    for i in range(len(df)):
        if df['KST_line'][i] > df['signal_line'][i]:
            if position == False:
                kst_buy.append(df['close'][i])
                kst_sell.append(np.nan)
                position = True
            else:
                kst_buy.append(np.nan)
                kst_sell.append(np.nan)
        elif df['KST_line'][i] < df['signal_line'][i]:
            if position == True:
                kst_buy.append(np.nan)
                kst_sell.append(df['close'][i])
                position = True
            else:
                kst_buy.append(np.nan)
                kst_sell.append(np.nan)
        else:
            kst_buy.append(np.nan)
            kst_sell.append(np.nan)
    print("KST Analysis completed")
    df['buy_signal_price'],df['sell_signal_price'] = pd.Series([kst_buy,kst_sell]) 
    df["strategy_name"]="KST_"+str(param_sma1)+"_"+str(param_sma2)+"_"+str(param_sma3)+"_"+str(param_sma4)+"_"+str(param_roc1)+"_"+str(param_roc2)+"_"+str(param_roc3)+"_"+str(param_roc4)+"_"+str(param_signal)
    df['indicator'] = "kst"
    print("TODO column rename") 
    return (df)


