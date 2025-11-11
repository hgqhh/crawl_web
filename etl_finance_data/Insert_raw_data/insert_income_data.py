import pandas as pd
import mysql.connector
from mysql.connector import Error
from vnstock import financial_flow
from datetime import datetime
# 3. Income Statement - Optimized
def incomeStatement(tickers):
    for ticker in tickers:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            database='stock',
            user='root',
            password='Clbtoanhoc48'
        )
        try:
            table = 'income'
            cursor = conn.cursor()
            
            # Truy vấn năm và quý mới nhất
            cursor.execute(f"SELECT MAX(year), MAX(quarter) FROM {table} WHERE ticker = '{ticker}'")
            result = cursor.fetchone()
            latest_year = result[0] if result[0] else 2010
            latest_quarter = result[1] if result[1] else 1
            
            print(f'Latest data for {ticker}: Year {latest_year}, Quarter {latest_quarter}')
            
            # Lấy báo cáo thu nhập
            new_record = financial_flow(symbol=ticker, report_type='incomestatement', report_range='quarterly')
            new_record.reset_index(inplace=True)
            new_record = new_record.fillna(0)
            
            # Tách year và quarter
            new_record[['year', 'quarter']] = new_record['index'].str.split('-Q', expand=True)
            new_record = new_record.drop('index', axis=1)
            new_record['year'] = new_record['year'].astype(int)
            new_record['quarter'] = new_record['quarter'].astype(int)
            
            print(f'Connected to database for {ticker}')
            
            if conn.is_connected():
                cursor = conn.cursor()
                inserted_count = 0
                
                for i, row in new_record.iterrows():
                    # Kiểm tra trùng lặp
                    check_sql = f"SELECT COUNT(*) FROM {table} WHERE ticker = %s AND year = %s AND quarter = %s"
                    cursor.execute(check_sql, (row['ticker'], row['year'], row['quarter']))
                    
                    if cursor.fetchone()[0] == 0:
                        if row['year'] > latest_year or (row['year'] == latest_year and row['quarter'] > latest_quarter):
                            sql = (
                                "INSERT INTO " + table + " VALUES ("
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                "%s, %s, %s, %s)"
                            )
                            cursor.execute(sql, tuple(row))
                            inserted_count += 1
                
                conn.commit()
                print(f'Inserted {inserted_count} new records for {ticker}')
            else:
                print("Not connected to MySQL")
                
        except Error as e:
            print(f'Error while inserting {ticker} to database', e)
        except Exception as ex:
            print(f'Error while extracting data for {ticker}', ex)
        finally:
            conn.close()

# Sử dụng
tickers = ['ACB']

# Gọi từng hàm
incomeStatement(tickers)