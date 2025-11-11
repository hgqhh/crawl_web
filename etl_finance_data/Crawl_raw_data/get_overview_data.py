import mysql.connector
from vnstock3 import Vnstock

# Kết nối tới cơ sở dữ liệu
connection = mysql.connector.connect(
    host='127.0.0.1',
    database='stock',
    user='root',
    password='Clbtoanhoc48'
)

cursor = connection.cursor()

companies = listing_companies()
ticker_list = companies['ticker'].tolist()

for ticker in ticker_list:
    try:
        # Kiểm tra trùng lặp
        cursor.execute("SELECT COUNT(*) FROM dim_overview WHERE ticker = %s", (ticker,))
        if cursor.fetchone()[0] > 0:
            print(f"{ticker} đã tồn tại trong cơ sở dữ liệu, bỏ qua.")
            continue

        company = Vnstock().stock(symbol=ticker, source='TCBS').company
        overview = company.overview()
        overview['ticker'] = ticker
        
        # Lọc các cột muốn lấy
        overview_filter = overview[['ticker','exchange','no_shareholders','foreign_percent','outstanding_share','issue_share','established_year','no_employees','stock_rating','delta_in_week','delta_in_month','delta_in_year','website']]
        
        # Chuẩn bị câu lệnh SQL để chèn dữ liệu vào cơ sở dữ liệu
        insert_query = """
        INSERT INTO dim_overview (ticker, exchange, no_shareholders, foreign_percent, outstanding_share, issue_share, established_year, no_employees, stock_rating, delta_in_week, delta_in_month, delta_in_year, website)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for index, row in overview_filter.iterrows():
            cursor.execute(insert_query, tuple(row))
            connection.commit()

        print(f"Đã chèn dữ liệu cho {ticker} vào cơ sở dữ liệu.")
    except Exception as e:
        print(f"Gặp lỗi với ticker {ticker}: {e}, bỏ qua ticker này.")

# Đóng kết nối
cursor.close()
connection.close()
