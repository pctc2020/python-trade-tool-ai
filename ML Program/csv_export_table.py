import pandas as pd
import mysql.connector

# Connect to MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root",
    database="stockmarket"
)

# Query data from the table
query = "SELECT * FROM trade_record_zenithbir"
cursor = conn.cursor()
cursor.execute(query)
data = cursor.fetchall()

# Get column names from cursor description
columns = [column[0] for column in cursor.description]

# Create DataFrame from fetched data and column names
df = pd.DataFrame(data, columns=columns)

# Save DataFrame to CSV file
df.to_csv('E:/python_intelora_code/ML Program/trade_record_zenithbir.csv', index=False)

# Close cursor and connection
cursor.close()
conn.close()
