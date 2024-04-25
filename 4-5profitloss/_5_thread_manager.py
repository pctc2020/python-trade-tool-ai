import mysql.connector
import threading
import time


def update_status_after_trade_success(oldStatus, newStatus, mydb):
    def update_thread():
        mycursor = mydb.cursor()
        sql_query = "UPDATE trade_data SET tradestatus = %s WHERE tradestatus = %s"
        try:
            mycursor.execute(sql_query, (newStatus, oldStatus))
            mydb.commit()
            print("Status updated successfully!")
        except Exception as e:
            print("Error:", e)
            print("Error in query:", sql_query)

        time.sleep(30)  # Wait for 30 seconds before processing the next row

    update_thread = threading.Thread(target=update_thread)
    update_thread.start()

    # Wait for the thread to finish execution
    update_thread.join()

def update_trade_status(old_status, new_status, mydb):
    try:
        # Fetch data from database
        mycursor = mydb.cursor()
        mycursor.execute("SELECT * FROM trade_data WHERE tradestatus = %s", (old_status,))
        rows = mycursor.fetchall()
        
        for row in rows:
            update_status_after_trade_success(old_status, new_status, mydb)

    except Exception as e:
        print("Error:", e)
    finally:
        if mydb:
            mydb.close()
