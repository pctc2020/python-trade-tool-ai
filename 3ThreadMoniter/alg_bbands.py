import pandas_ta as ta
import numpy as np
import pandas as pd
# import executor


#param: it require 2 parameters length and std
def bbands_buy_sell(data, params):
    print("Starting bbands Analysis ==== ")
    param1_length = int(params[0])
    param2_std = float(params[1])
    bbands = ta.bbands(data['close'] , length=param1_length, std=param2_std) 
    # bbands.rename(columns = {"BBL_20_2":"col1","BBU_20_2":"col2"},inplace=True)
    bbandsBuy = []
    bbandsSell = []
    position = False 
    data = pd.concat([data,bbands],axis=1).reindex(data.index)
    
    #risk = "BBL_10_1.5"
    #risk = "BBL_"+str(param1_length)+"_2.0"
    risk = "BBL_"+str(param1_length)+"_"+str(param2_std)
    for i in range(len(data)):
        if data['close'][i] < data[risk][i]: 
            if position == False :
                bbandsBuy.append(data['close'][i])
                bbandsSell.append(np.nan)
                position = True
            else:
                bbandsBuy.append(np.nan)
                bbandsSell.append(np.nan)
        elif data['close'][i] > data[risk][i]:
            if position == True:
                bbandsBuy.append(np.nan)
                bbandsSell.append(data['close'][i])
                position = False
            else:
                bbandsBuy.append(np.nan)
                bbandsSell.append(np.nan)
        else:
            bbandsBuy.append(np.nan)
            bbandsSell.append(np.nan)
    print("Sucessfully Completed bbands Analysis")
    data['buy_signal_price'], data['sell_signal_price'] = pd.Series([bbandsBuy, bbandsSell])
    data["strategy_name"]="bbands_{}".format(param1_length)
    data['indicator'] = "bbands"
    print("TODO column rename") 
    return (data)