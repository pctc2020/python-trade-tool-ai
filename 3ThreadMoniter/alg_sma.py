import pandas_ta as ta
import numpy as np
import pandas as pd
import utils
# import executor

# Function for buy and sell signal using SMA
# use 2 params signal1 and signal2
def sma_buy_sell(data, values):
    print("Starting SMA Analysis")
    signal1 = values[0]
    signal2 = values[1]

    sma1 = ta.sma(data['close'], signal1)
    sma2 = ta.sma(data['close'], signal2)

    signalBuy = []
    signalSell = []
    position = False

    for i in range(len(data)):
        if sma1[i] > sma2[i]:
            if not position:
                signalBuy.append(data['close'][i])
                signalSell.append(np.nan)
                position = True
            else:
                signalBuy.append(np.nan)
                signalSell.append(np.nan)
        elif sma1[i] < sma2[i]:
            if position:
                signalBuy.append(np.nan)
                signalSell.append(data['close'][i])
                position = False
            else:
                signalBuy.append(np.nan)
                signalSell.append(np.nan)
        else:
            signalBuy.append(np.nan)
            signalSell.append(np.nan)

    print("Successfully Completed SMA Analysis")
    data['buy_signal_price'], data['sell_signal_price'] = pd.Series([signalBuy, signalSell])
    data["strategy_name"] = "sma_{}_{}".format(signal1, signal2)
    data['indicator'] = "sma"
    data['tradestatus'] = "Completed"
    print("TODO column rename") 
    utils.update_data_table(data)
    return (data)
