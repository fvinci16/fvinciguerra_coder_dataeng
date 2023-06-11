import yfinance as yf
import redshift_connector
from Parametros_conexión import host, database, port, user, password

# Tomo la data historica de google
goo = yf.Ticker('GOOG')
hist = goo.history(period="1y")
hist['Date'] = hist.index
hist = hist.reset_index(drop=True)

# Conecto a redshift - Invoco variables
connection = redshift_connector.connect(
    host=host,
    database=database,
    port=port,
    user=user,
    password=password
)

# selecciono mi esquema en la base de datos
my_schema = "fvinciguerra_coderhouse"

# Crear tabla si no existe
create_table_query = """
    CREATE TABLE IF NOT EXISTS {}.stock_data (
        Date DATE,
        "Open" DECIMAL,
        High DECIMAL,
        Low DECIMAL,
        Close DECIMAL,
        Volume DECIMAL,
        Dividends DECIMAL,
        Stock_Splits DECIMAL
    )
""".format(my_schema)

cursor = connection.cursor()
cursor.execute(create_table_query)

# Trunco la tabla "stock_data" para actualizar la información
truncate_table_query = """
    TRUNCATE TABLE {}.stock_data
""".format(my_schema)

cursor.execute(truncate_table_query)

# Inserto los valores
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

print("El insert a la tabla 'stock_data' se completó exitosamente.")
