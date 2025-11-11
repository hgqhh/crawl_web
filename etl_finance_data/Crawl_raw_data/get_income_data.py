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
def incomeStatement(tickers):
    for ticker in tickers:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            database='stock',
            user='root',
            password='Clbtoanhoc48'
        )
        try:
            # Lấy báo cáo tài chính
            new_record = financial_flow(symbol=ticker, report_type='incomestatement', report_range='quarterly')
            new_record.reset_index(inplace=True)
            new_record = new_record.fillna(0)
            new_record[['year', 'quarter']] = new_record['index'].str.split('-Q', expand=True)
            new_record = new_record.drop('index', axis=1)

            print(f'Connected to database for {ticker}')
            try:
                if conn.is_connected():
                    table = 'income'
                    cursor = conn.cursor()

                    # Duyệt qua từng dòng dữ liệu và chèn vào MySQL
                    for i, row in new_record.iterrows():
                        sql = (
                            "INSERT INTO " + table + " VALUES ("
                            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                            "%s, %s, %s, %s)"
                        )
                        cursor.execute(sql, tuple(row))
                    conn.commit()
                else:
                    print("Not connected to MySQL")
                    break
            except Error as e:
                print(f'Error while inserting {ticker} to database', e)
            finally:
                conn.close()

        except Exception as ex:
            print(f'Error while extracting data for {ticker}', ex)

# Danh sách các ticker
tickers = [
    'ACB'
]
# Gọi hàm xử lý dữ liệu
incomeStatement(tickers)