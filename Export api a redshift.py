import yfinance as yf
import redshift_connector

# Tomo la data historica de google
goo = yf.Ticker('GOOG')
hist = goo.history(period="1y")
hist['Date'] = hist.index
hist = hist.reset_index(drop=True)

# Conecto a redshift
connection = redshift_connector.connect(
    host='@host',
    database='@database',
    port=5439,
    user='@user',
    password='@pass'
)

my_schema = "fvinciguerra_coderhouse"

cursor = connection.cursor()
insert_query = """
    INSERT INTO {}.stock_data (Date, "Open", High, Low, Close, Volume, Dividends, Stock_Splits)
    VALUES (TO_DATE(%s, 'YYYY-MM-DD'), %s, %s, %s, %s, %s, %s, %s)
""".format(my_schema)

for index, row in hist.iterrows():
    values = (
        row['Date'].strftime('%Y-%m-%d'),
        row['Open'],
        row['High'],
        row['Low'],
        row['Close'],
        row['Volume'],
        row['Dividends'],
        row['Stock Splits']
    )
    cursor.execute(insert_query, values)

# Commit y cierre de conexión
connection.commit()
connection.close()
