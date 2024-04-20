import mysql.connector

# Connect to the MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root",
    database="stockmarket"
)

# Create a cursor object to execute SQL commands
cursor = mydb.cursor()

def createORdeleteTable(tableNaam):
    # Define the SQL CREATE TABLE statement
    create_table_query = """
    CREATE TABLE IF NOT EXISTS """ + tableNaam + """ (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(45) NOT NULL,
    department VARCHAR(45) NOT NULL
    )
    """

    # Execute the CREATE TABLE statement
    cursor.execute(create_table_query)

    # Commit the transaction
    mydb.commit()
    print("Table Created!!!")

def main():
    tableNaam = "temp1"
    createORdeleteTable(tableNaam)

# Call the main function
if __name__ == "__main__":
    main()

# Close the cursor and connection
cursor.close()
mydb.close()
