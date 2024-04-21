import pandas as pd
import numpy as np
import pandas_ta as ta
import utils

# Import the executor module if needed
# need 1 parameter risk
def adx_buy_sell(df, risk):
    risk = float(risk[0])  # Convert the risk parameter to an integer
    print("Starting ADX analysis")
    
    adx_buy = []  # List to store buy signals
    adx_sell = []  # List to store sell signals
    position = False  # Position flag to track whether we are in a position or not

    # Calculate the ADX using the pandas_ta library, specifying the length parameter as risk
    adx = ta.adx(df['high'], df['low'], df['close'],risk)
    df = pd.concat([df, adx], axis=1).reindex(df.index)  # Add ADX values to the DataFrame
    
    for i in range(len(df)):
        # Check if ADX value exists in the DataFrame before using it
        if not np.isnan(df["ADX_{}".format(risk)][i]):
            if df["ADX_{}".format(risk)][i] > 25 and df["DMP_{}".format(risk)][i] > df["DMN_{}".format(risk)][i]:
                # Buy condition: ADX > 25 and DMP > DMN
                if not position:  # If not already in a position
                    adx_buy.append(df['close'][i])  # Append buy price
                    adx_sell.append(np.nan)  # No sell signal, so append NaN
                    position = True  # Set position to True to indicate being in a position
                else:
                    adx_buy.append(np.nan)  # No buy signal, so append NaN
                    adx_sell.append(np.nan)  # No sell signal, so append NaN
            elif df["ADX_{}".format(risk)][i] > 25 and df["DMN_{}".format(risk)][i] > df["DMP_{}".format(risk)][i]:
                # Sell condition: ADX > 25 and DMN > DMP
                if position:  # If already in a position
                    adx_buy.append(np.nan)  # No buy signal, so append NaN
                    adx_sell.append(df['close'][i])  # Append sell price
                    position = False  # Set position to False to indicate not being in a position
                else:
                    adx_buy.append(np.nan)  # No buy signal, so append NaN
                    adx_sell.append(np.nan)  # No sell signal, so append NaN
            else:
                # No buy or sell signal
                adx_buy.append(np.nan)
                adx_sell.append(np.nan)
        else:
            # ADX value is NaN, so no buy or sell signal
            adx_buy.append(np.nan)
            adx_sell.append(np.nan)
    
    print("ADX Analysis completed")

    # Add buy and sell signals as new columns to the DataFrame
    df['buy_signal_price'], df['sell_signal_price'] = pd.Series([adx_buy, adx_sell])

    # Set strategy name and indicator columns
    df["strategy_name"] = "adx_{}".format(risk)
    df['indicator'] = "adx"
    df['tradestatus'] = "Completed"
    print("TODO column rename")
    utils.update_data_table(df)
    return df