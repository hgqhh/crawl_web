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
# Lấy danh sách các công ty niêm yết và chỉ lấy 13 cột đầu tiên
companies = listing_companies()
filtered_companies = companies.iloc[:, :13]
print(filtered_companies)
# Thiết lập kết nối với MySQL
connection = mysql.connector.connect(
    host='127.0.0.1',
    database='stock',
    user='root',
    password='Clbtoanhoc48'
)

cursor = connection.cursor()
table = 'company'
for i, row in filtered_companies.iterrows():
    sql = (
        "INSERT INTO " + 'company' + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    cursor.execute(sql, tuple(row))

# Xác nhận thay đổi
connection.commit()

# Đóng kết nối
cursor.close()
connection.close()

print("Dữ liệu đã được đẩy lên MySQL thành công.")