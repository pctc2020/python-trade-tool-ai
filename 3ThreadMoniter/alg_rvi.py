import pandas as pd
import numpy as np
import pandas_ta as ta
# import executor

#param: it require 2 parameters period and signal
def rvi_buy_sell(data, risk):
    risk1_period = int(risk[0])
    risk2_signal = float(risk[1])
    rvi_buy = []
    rvi_sell = []
    position = False
    rvi = ta.rvi(data['close'],data['high'],data['low'], length=risk1_period, scalar=risk2_signal)
    data = pd.concat([data, rvi], axis=1).reindex(data.index)
    for i in range(len(data)):
        if data['RVI_'+str(risk1_period)][i] > 50 and i > 3:
            if position == False:
                rvi_buy.append(data['close'][i])
                rvi_sell.append(np.nan)
                position = True
            else:
                rvi_buy.append(np.nan)
                rvi_sell.append(np.nan)
        elif data['RVI_'+str(risk1_period)][i] < 50 and i > 3:
            if position == True:
                rvi_buy.append(np.nan)
                rvi_sell.append(data['close'][i])
                position = False
            else:
                rvi_buy.append(np.nan)
                rvi_sell.append(np.nan)
        else:
            rvi_buy.append(np.nan)
            rvi_sell.append(np.nan)
    print("Sucessfully Completed RVI Analysis")
    data['buy_signal_price'], data['sell_signal_price'] = pd.Series([rvi_buy, rvi_sell])
    data["strategy_name"]="RVI_{}".format(risk1_period)
    data['indicator'] = "RVI"
    print("TODO column rename") 
    return data


            