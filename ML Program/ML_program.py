import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn import metrics
import matplotlib.pyplot as plt
import math
import mysql.connector
from ta.momentum import AwesomeOscillatorIndicator, RSIIndicator, TSIIndicator
from ta.trend import EMAIndicator, SMAIndicator, KSTIndicator

def db_connection():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="Root",
        database="stockmarket"
    )
    return mydb

def handle_nan_values(df):
    # print(df)
    df.fillna(0, inplace=True)
    # print(df)
# --------------------------------------------------------------------------------
def multiple_table(mydb):
    db_cursor = mydb.cursor()
    query = "SHOW TABLES LIKE 'trade_record%'"
    db_cursor.execute(query)    
    table_names = [name[0] for name in db_cursor.fetchall()]
    print(table_names)
    return table_names

def df_data(mydb, table_names):
    all_dfs = []
    db_cursor=mydb.cursor()

    for table_name in table_names:
        query1 = "SELECT * FROM {}".format(table_name)
        db_cursor.execute(query1)
    
        trade_records = db_cursor.fetchall()
        columns = [i[0] for i in db_cursor.description]
        combined_df = pd.DataFrame(trade_records, columns=columns)
        all_dfs.append(combined_df)
        
    df = pd.concat(all_dfs, ignore_index=True)
    print(df)
    return df

def Data_cleaning(df):
    print(df.head(10))

    # Display the shape of the dataframe
    print(df.shape)

    # Drop unnecessary columns
    df.drop(['id','last_price', 'prev_close', 'traded_value', '52W_high', '52 Week Low Price'], axis=1, inplace=True)

    # Convert 'tradetime2' column to datetime format
    df['Date'] = pd.to_datetime(df['tradetime2'], format='%d-%m-%Y')
    
    # Display the modified dataframe
    print("Print Data After Drop Cloumns")
    print(df.head())
    return df

def indicator(df, indicator_name):
    indicator_functions = {
    'AO': AwesomeOscillatorIndicator,
    'EMA': EMAIndicator,
    'RSI': RSIIndicator,
    'SMA': SMAIndicator,
    'TSI': TSIIndicator,
    'KST': KSTIndicator
    }

    # Name of the indicator to run
    # indicator_name = 'EMA'

    # Calculate the selected indicator
    indicator_class = indicator_functions.get(indicator_name)
    if indicator_class:
        if indicator_name == 'AO':
            indicator_fun = indicator_class(high=df['high'], low=df['low'])
            indicator = indicator_fun.awesome_oscillator()
        elif indicator_name == 'EMA':
            indicator_fun = indicator_class(df['close'])
            indicator = indicator_fun.ema_indicator()
        elif indicator_name == 'RSI':
            indicator_fun = indicator_class(df['close'])
            indicator = indicator_fun.rsi()
        elif indicator_name == 'SMA':
            indicator_fun = indicator_class(df['close'])
            indicator = indicator_fun.sma_indicator()
        elif indicator_name == 'TSI':
            indicator_fun = indicator_class(df['close'])
            indicator = indicator_fun.tsi()
        elif indicator_name == 'KST':
            indicator_fun = indicator_class(df['close'])
            indicator = indicator_fun.kst()
        else:
            print("This Indicator is not available...")
        df[indicator_name] = indicator
    handle_nan_values(df)
    return df

def ML_Prediction(df, indicator_name):    
    # Linear regression algorithm
    x = df[['open', 'high', 'low', 'quantity', indicator_name]]
    y = df['close']

    # Split the dataset into train and test sets
    x_train , x_test , y_train, y_test = train_test_split(x, y, random_state=0)
    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    # Create and train the linear regression model
    regressor = LinearRegression()
    regressor.fit(x_train, y_train)

    # Display the coefficients and intercept of the model
    print('Coefficients:', regressor.coef_)
    print('Intercept:', regressor.intercept_)

    # Make predictions
    predicted = regressor.predict(x_test)

    # Display the test dataset
    print(x_test)

    # Create DataFrame to compare actual and predicted values
    dfr = pd.DataFrame({'Actual': y_test, 'Predicted': predicted})
    print(dfr)

    # Calculate and print evaluation metrics
    print('Mean Absolute Error:', metrics.mean_absolute_error(y_test, predicted))
    print('Mean Squared Error:', metrics.mean_squared_error(y_test, predicted))
    print('Root Mean Squared Error:', math.sqrt(metrics.mean_squared_error(y_test, predicted)))

    # Create bar plot to visualize actual vs predicted values
    graph = dfr.head(9)
    graph.plot(kind='bar')

def main():
    mydb = db_connection()
    print("Database Connected!")
    table_names = multiple_table(mydb)
    data = df_data(mydb, table_names)
    df1 = Data_cleaning(data)
    print(" Data Cleaning Completed!")
    # Name of the indicator to run
    indicator_name = 'EMA'
    df = indicator(df1, indicator_name)
    ML_Prediction(df, indicator_name)

main()


# ------------------------------------ML Code Without Functions----------------------------------------------

# # Load the dataset
# # df = pd.read_csv('E:/python_intelora_code/ML Program/trade_record_zenithbir.csv')
# mydb = db_connection()
# table_names = multiple_table(mydb)
# df = df_data(mydb, table_names)
# # Display the first 10 rows of the dataframe
# print(df.head(10))

# # Display the shape of the dataframe
# print(df.shape)

# # Drop unnecessary columns
# df.drop(['id','last_price', 'prev_close', 'traded_value', '52W_high', '52 Week Low Price'], axis=1, inplace=True)

# # Convert 'tradetime2' column to datetime format
# df['Date'] = pd.to_datetime(df['tradetime2'], format='%d-%m-%Y')

# # Display the modified dataframe
# print("Print Data After Drop Cloumns")
# print(df.head())

# # Drop 'id' column
# # df.drop('id', axis=1, inplace=True)

# # # Display the shape of the dataframe after dropping columns
# # print(df.shape)

# # # Check for missing values
# # print(df.isnull().sum())

# # # Check for NaN values
# # print(df.isna().any())

# # # Display the information about the dataframe
# # print(df.info())

# # # Display descriptive statistics of the dataframe
# # print(df.describe())

# # Plot the 'open' column
# df['open'].plot(figsize=(16,6)) # Graph 

# # Define a dictionary to map indicator names to their calculation functions
# indicator_functions = {
#     'AO': AwesomeOscillatorIndicator,
#     'EMA': EMAIndicator,
#     'RSI': RSIIndicator,
#     'SMA': SMAIndicator,
#     'TSI': TSIIndicator,
#     'KST': KSTIndicator
# }

# # Name of the indicator to run
# indicator_name = 'EMA'

# # Calculate the selected indicator
# indicator_class = indicator_functions.get(indicator_name)
# if indicator_class:
#     if indicator_name == 'AO':
#         indicator_fun = indicator_class(high=df['high'], low=df['low'])
#         indicator = indicator_fun.awesome_oscillator()
#     elif indicator_name == 'EMA':
#         indicator_fun = indicator_class(df['close'])
#         indicator = indicator_fun.ema_indicator()
#     elif indicator_name == 'RSI':
#         indicator_fun = indicator_class(df['close'])
#         indicator = indicator_fun.rsi()
#     elif indicator_name == 'SMA':
#         indicator_fun = indicator_class(df['close'])
#         indicator = indicator_fun.sma_indicator()
#     elif indicator_name == 'TSI':
#         indicator_fun = indicator_class(df['close'])
#         indicator = indicator_fun.tsi()
#     elif indicator_name == 'KST':
#         indicator_fun = indicator_class(df['close'])
#         indicator = indicator_fun.kst()
#     else:
#         print("This Indicator is not available...")
#     df[indicator_name] = indicator

# # print(df.head())
# # print(df.columns)
# # print(df['KST'])
# handle_nan_values(df)

# # Linear regression algorithm
# x = df[['open', 'high', 'low', 'quantity', indicator_name]]
# y = df['close']

# # Split the dataset into train and test sets
# x_train , x_test , y_train, y_test = train_test_split(x, y, random_state=0)
# # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
# # Create and train the linear regression model
# regressor = LinearRegression()
# regressor.fit(x_train, y_train)

# # Display the coefficients and intercept of the model
# print('Coefficients:', regressor.coef_)
# print('Intercept:', regressor.intercept_)

# # Make predictions
# predicted = regressor.predict(x_test)

# # Display the test dataset
# print(x_test)

# # Create DataFrame to compare actual and predicted values
# dfr = pd.DataFrame({'Actual': y_test, 'Predicted': predicted})
# print(dfr)

# # Calculate and print evaluation metrics
# print('Mean Absolute Error:', metrics.mean_absolute_error(y_test, predicted))
# print('Mean Squared Error:', metrics.mean_squared_error(y_test, predicted))
# print('Root Mean Squared Error:', math.sqrt(metrics.mean_squared_error(y_test, predicted)))

# # Create bar plot to visualize actual vs predicted values
# graph = dfr.head(9)
# graph.plot(kind='bar')
# --------------------------------New Program -----------------------------------------------
# import pandas as pd
# from ta.momentum import AwesomeOscillatorIndicator, RSIIndicator
# from ta.trend import EMAIndicator, SMAIndicator
# from ta.others import TSIIndicator, KSTIndicator
# from sklearn.model_selection import train_test_split
# from sklearn.linear_model import LinearRegression
# from sklearn.metrics import mean_squared_error

# # Sample DataFrame with stock data
# data = {
#     'Date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04'],
#     'Open': [100, 105, 110, 115],
#     'High': [105, 110, 115, 120],
#     'Low': [95, 100, 105, 110],
#     'Close': [102, 108, 112, 118],
#     'Quantity': [1000, 1200, 1500, 800]  # Sample quantity data
# }

# df = pd.DataFrame(data)

# # Define a dictionary to map indicator names to their calculation functions
# indicator_functions = {
#     'AO': AwesomeOscillatorIndicator,
#     'EMA': EMAIndicator,
#     'RSI': RSIIndicator,
#     'SMA': SMAIndicator,
#     'TSI': TSIIndicator,
#     'KST': KSTIndicator
# }

# # Name of the indicator to run
# indicator_name = 'AO'

# # Calculate the selected indicator
# indicator_class = indicator_functions.get(indicator_name)
# if indicator_class:
#     if indicator_name == 'AO':
#         indicator = indicator_class(high=df['High'], low=df['Low'])
#     else:
#         indicator = indicator_class(df['Close'])
#     df[indicator_name] = indicator

# # Input features (including the selected indicator)
# if indicator_name == 'AO':
#     X = df[['Open', 'High', 'Low', 'Quantity', indicator_name]]
# else:
#     X = df[['Open', 'High', 'Low', 'Close', 'Quantity', indicator_name]]

# # Target variable
# y = df['Close']

# # Splitting data into training and testing sets
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# # Training the model
# model = LinearRegression()
# model.fit(X_train, y_train)

# # Making predictions
# y_pred = model.predict(X_test)

# # Evaluating the model
# mse = mean_squared_error(y_test, y_pred)
# print("Mean Squared Error:", mse)
