import pandas as pd
import mysql.connector
from mysql.connector import Error
from vnstock import financial_ratio
from datetime import datetime

def ratio(tickers):
    for ticker in tickers:
        # Kết nối tới cơ sở dữ liệu MySQL
        conn = mysql.connector.connect(
            host='127.0.0.1',
            database='stock',
            user='root',
            password='Clbtoanhoc48'
        )
        try:
            table = 'ratio'
            cursor = conn.cursor()
            
            # Truy vấn ngày mới nhất của mã cổ phiếu từ bảng 'ratio'
            cursor.execute(f"SELECT MAX(yearReport) FROM {table} WHERE ticker = '{ticker}'")
            latest_date = cursor.fetchone()[0]
            
            if latest_date:
                latest_date = str(latest_date)[:10]
            else:
                latest_date = '2010-01-01'
            
            print(f'Latest date for {ticker}: {latest_date}')
            
            # Lấy dữ liệu financial ratios quarterly
            new_record = financial_ratio(ticker, 'quarterly', True)
            new_record = new_record.fillna(0)
            
            # Transpose the DataFrame
            new_record = new_record.transpose()
            new_record.reset_index(inplace=True)
            new_record = new_record.rename(columns={'index': 'metric'})
            
            print(f'Connected to database for {ticker}')
            
            try:
                if conn.is_connected():
                    cursor = conn.cursor()
                    
                    for i, row in new_record.iterrows():
                        # Kiểm tra xem dữ liệu đã tồn tại chưa
                        # Giả sử cột đầu tiên là ticker và cột thứ 2 là yearReport
                        check_sql = f"SELECT COUNT(*) FROM {table} WHERE ticker = %s AND yearReport = %s AND metric = %s"
                        cursor.execute(check_sql, (row[0], row[1], row['metric']))
                        
                        if cursor.fetchone()[0] == 0:
                            # Chèn dữ liệu mới nếu chưa tồn tại
                            sql = (
                                "INSERT INTO " + table + " VALUES (" +
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " +
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " +
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " +
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            )
                            cursor.execute(sql, tuple(row))
                        
                    conn.commit()
                    print(f'Successfully inserted data for {ticker}')
                else:
                    print("Not connected to MySQL")
                    break
                    
            except Error as e:
                print(f'Error while inserting {ticker} to database', e)
                continue
                
        except Exception as ex:
            print(f'Error while extracting data for {ticker}', ex)
            
        finally:
            conn.close()

# Danh sách các mã cổ phiếu
tickers = ['ACB']
ratio(tickers)