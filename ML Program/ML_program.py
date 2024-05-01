import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn import metrics
import matplotlib.pyplot as plt
import math

# Load the dataset
df = pd.read_csv('E:/python_intelora_code/ML Program/trade_record_zenithbir.csv')

# Display the first 10 rows of the dataframe
print(df.head(10))

# Display the shape of the dataframe
print(df.shape)

# Drop unnecessary columns
df.drop(['last_price', 'prev_close', 'traded_value', '52W_high', '52 Week Low Price'], axis=1, inplace=True)

# Display the modified dataframe
print(df.head())

# Convert 'tradetime2' column to datetime format
df['Date'] = pd.to_datetime(df['tradetime2'], format='%d-%m-%Y')

# Drop 'tradetime2_datetime' column
# df.drop('tradetime2_datetime', axis=1, inplace=True)

# Drop 'id' column
df.drop('id', axis=1, inplace=True)

# Display the shape of the dataframe after dropping columns
print(df.shape)

# Check for missing values
print(df.isnull().sum())

# Check for NaN values
print(df.isna().any())

# Display the information about the dataframe
print(df.info())

# Display descriptive statistics of the dataframe
print(df.describe())

# Plot the 'open' column
df['open'].plot(figsize=(16,6))

# Linear regression algorithm
x = df[['open', 'high', 'low', 'quantity']]
y = df['close']

# Split the dataset into train and test sets
x_train , x_test , y_train, y_test = train_test_split(x, y, random_state=0)

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
