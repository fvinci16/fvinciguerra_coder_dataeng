import yfinance as yf
import redshift_connector
import pandas as pd
from Parametros_conexión import host, database, port, user, password

# Tomo la data historica de Google (1 Año)
goo = yf.Ticker('GOOG')
hist = goo.history(period="1y")

# Reseteo de index - Es necesario para el calculo de Month Volume
hist = hist.reset_index()

# Convierto 'Date' a Datetype para luego calcular el volumen por mes
hist['Date'] = pd.to_datetime(hist['Date'])

# Si stock_splits no tiene dato, le pongo un "0".
if 'Stock_Splits' not in hist.columns:
    hist['Stock_Splits'] = 0
else:
    hist['Stock_Splits'] = hist['Stock_Splits'].fillna(0)

# Remuevo duplicados si es que hay
hist = hist.drop_duplicates(subset=['Date'], keep='last')

# Calculo de volumen por mes
hist['MonthVolume'] = hist.groupby(pd.Grouper(key='Date', freq='M'))['Volume'].transform('sum')

# Conexión a Redshift
connection = redshift_connector.connect(
    host=host,
    database=database,
    port=port,
    user=user,
    password=password
)

# Seteo mi esquema
my_schema = "fvinciguerra_coderhouse"

# Creo la tabla si no existe
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {my_schema}.stock_data (
        Date DATE,
        "Open" DECIMAL,
        High DECIMAL,
        Low DECIMAL,
        Close DECIMAL,
        Volume DECIMAL,
        Dividends DECIMAL,
        Stock_Splits DECIMAL,
        MonthVolume DECIMAL(30, 2)
    )
"""

cursor = connection.cursor()
cursor.execute(create_table_query)

# Trunco "stock_data" para hacer un update de la info
truncate_table_query = f"""
    TRUNCATE TABLE {my_schema}.stock_data
"""
cursor.execute(truncate_table_query)

# Insert de los valores
insert_query = f"""
    INSERT INTO {my_schema}.stock_data
    (Date, "Open", High, Low, Close, Volume, Dividends, Stock_Splits, MonthVolume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

values = hist[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits', 'MonthVolume']].values

with connection.cursor() as cursor:
    cursor.executemany(insert_query, values)

# Commit y cierre de conexión
connection.commit()
connection.close()

# Print para chequear status
print("El insert de 'stock_data' se completó exitosamente")
