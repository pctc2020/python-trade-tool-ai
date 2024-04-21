import pandas_ta as ta
import numpy as np
import pandas as pd
import utils
# import executor
#SMA BUY SELL
#Function for buy and sell signal

def ema_buy_sell(data, values):
    print("Starting EMA Analysis")
    signal1 = values[0]
    signal2 = values[1]
    data["col1"] = ta.ema(data['close'],signal1)
    data["col2"] = ta.ema(data['close'],signal2)
    signalBuy = []
    signalSell = []
    position = False 

    for i in range(len(data)):
        if data["col1"][i] > data["col2"][i]:
            if position == False :
                signalBuy.append(data['close'][i])
                signalSell.append(np.nan)
                position = True
            else:
                signalBuy.append(np.nan)
                signalSell.append(np.nan)
        elif data["col1"][i] < data["col2"][i]:
            if position == True:
                signalBuy.append(np.nan)
                signalSell.append(data['close'][i])
                position = False
            else:
                signalBuy.append(np.nan)
                signalSell.append(np.nan)
        else:
            signalBuy.append(np.nan)
            signalSell.append(np.nan)
    print("Sucessfully Completed EMA Analysis")
    data['buy_signal_price'], data['sell_signal_price'] = pd.Series([signalBuy, signalSell])
    data["strategy_name"]="ema_{}_{}".format(signal1,signal2)
    data['indicator'] = "ema"
    data['tradestatus'] = "Completed"
    print("TODO column rename") 
    utils.update_data_table(data)
    return (data)

    

