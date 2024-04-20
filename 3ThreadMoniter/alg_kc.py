import pandas as pd
import numpy as np
import pandas_ta as ta
# import executor

#it need 2 parameter period and multiple
def kc_buy_sell(df, risk):
    risk1_sma_period = int(risk[0])
    risk2_atr_multiple= float(risk[1])
    
    print("starting KC analysis")
    kc_buy = []
    kc_sell = []
    position = False
    #kc = ta.kc(df['high'], df['low'], df['close'], length=sma_period, atr_length=sma_period, atr_multiplier=atr_multiple)
    kc = ta.kc(df['high'],df['low'],df['close'], length=risk1_sma_period, scalar=risk2_atr_multiple)
    df = pd.concat([df, kc], axis=1).reindex(df.index)
    colnamepart = '_'+str(risk1_sma_period)+'_'+str(risk2_atr_multiple)
    for i in range(len(df)):
        if df['close'][i] < df['KCLe'+colnamepart][i]:
            if position == False:
                kc_buy.append(df['close'][i])
                kc_sell.append(np.nan)
                position = True
            else:
                kc_buy.append(np.nan)
                kc_sell.append(np.nan)
        elif df['close'][i] > df['KCUe'+colnamepart][i]:
            if position == True:
                kc_buy.append(np.nan)
                kc_sell.append(df['close'][i])
                position = False
            else:
                kc_buy.append(np.nan)
                kc_sell.append(np.nan)
        else:
            kc_buy.append(np.nan)
            kc_sell.append(np.nan)
    print("KC Analysis completed")
    df['buy_signal_price'], df['sell_signal_price'] = pd.Series([kc_buy, kc_sell])
    df["strategy_name"]="kc_"+colnamepart
    df['indicator'] = "kc"
    print("TODO column rename") 
    return (df)
    