import pandas as pd
import numpy as np
import pandas_ta as ta
# import executor

# Function to calculate TSI and generate buy/sell signals
#param: it require 3 parameters fast, slow and signal
def tsi_buy_sell(df, param):
    param_fast = int(param[0])
    param_slow = int(param[1])
    param_signal = int(param[2])
   
    
    print("Starting TSI analysis")
    tsi_buy = []
    tsi_sell = []
    position = False
    
    # Calculate the TSI indicator
    tsi = ta.tsi(df['close'], fast=param_fast, slow=param_slow, signal=param_signal)
    
    # Concatenate TSI to DataFrame and rename columns
    df = pd.concat([df, tsi], axis=1).reindex(df.index)
    
    # Generate buy/sell signals based on TSI
    for i in range(len(df)):
        if df['TSI_'+str(param_fast)+'_'+str(param_fast)+'_'+str(param_signal)][i] > df['TSIs_'+str(param_fast)+'_'+str(param_fast)+'_'+str(param_signal)][i]:
            if not position:
                tsi_buy.append(df['close'][i])
                tsi_sell.append(np.nan)
                position = True
            else:
                tsi_buy.append(np.nan)
                tsi_sell.append(np.nan)
        elif df['TSI_'+str(param_fast)+'_'+str(param_fast)+'_'+str(param_signal)][i] < df['TSIs_'+str(param_fast)+'_'+str(param_fast)+'_'+str(param_signal)][i]:
            if position:
                tsi_buy.append(np.nan)
                tsi_sell.append(df['close'][i])
                position = False
            else:
                tsi_buy.append(np.nan)
                tsi_sell.append(np.nan)
        else:
            tsi_buy.append(np.nan)
            tsi_sell.append(np.nan)
    
    print("TSI Analysis completed")
    
    # Add buy/sell signals, strategy name, and indicator to DataFrame
    df['buy_signal_price'], df['sell_signal_price'] = pd.Series([tsi_buy, tsi_sell])
    df["strategy_name"] = "tsi_{}_{}_{}".format(param_slow, param_fast, param_signal)
    df['indicator'] = "tsi"
    print("TODO column rename") 
    return (df)
