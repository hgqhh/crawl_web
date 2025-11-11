import pandas as pd
import mysql.connector
from mysql.connector import Error
from vnstock import listing_companies
from datetime import datetime
from vnstock import stock_historical_data
from vnstock import financial_flow
from vnstock import financial_ratio
from vnstock import financial_report
from sqlalchemy import create_engine
def ratio(tickers):
    for ticker in tickers:
        # Connect to MySQL database
        conn = mysql.connector.connect(
            host='127.0.0.1',
            database='stock',
            user='root',
            password='Clbtoanhoc48'
        )
        try:
            # Fetch financial ratios quarterly
            new_record = financial_ratio(ticker, 'quarterly', True)
            new_record = new_record.fillna(0)
            print(f'Connected to database for {ticker}')
            #print(new_record)
            # Transpose the DataFrame
            new_record = new_record.transpose()  # Transpose the DataFrame
            new_record.reset_index(inplace=True)  # Reset the index
            new_record = new_record.rename(columns={'index': 'metric'})  # Rename index column if needed
            #print(new_record)
            try:
                if conn.is_connected():
                    table = 'ratio'
                    cursor = conn.cursor()

                    # Iterate through each row and insert into MySQL
                    for i, row in new_record.iterrows():
                        sql = (
                            "INSERT INTO " + table + " VALUES (" +
                            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " +
                            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " +
                            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " +
                            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        )
                        cursor.execute(sql, tuple(row))
                    conn.commit()
                else:
                    print("Not connected to MySQL")
                    break
            except Error as e:
                print(f'Error while inserting {ticker} to database', e)
                continue  # Move to the next ticker if there's an error
            finally:
                conn.close()  # Ensure connection is closed after operations

        except Exception as ex:
            print(f'Error while extracting data for {ticker}', ex)

# List of stock tickers
tickers = [
    'ACB'
]
# tickers = [
#     'HPT', 'ICT', 
#     'PMJ', 'PMT', 'SBD', 
#     'poVIE', 'VTE'
# ]

# Call the function to process data
ratio(tickers)