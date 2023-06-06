Proyecto de extracción desde API yahoo a una base de datos en Amazon Redshift-

Para correr este proyecto es necesario tener las siguientes librerias instaladas:
yfinance (api de yahoo)
redshift_connector (connector a redshift)

Los datos de conexión se encuentran variabilizados y se ingresan desde el .py "Parametros_conexión"

----

Para inserción de datos bulk es posible utilizar el método COPY - para ello es necesario tener instalado psicopg2


# Defino el nombre de la tabla
table_name = "{}.stock_data".format(my_schema)

# Creo un objeto StringIO para almacenar los datos
data_buffer = io.StringIO()
hist.to_csv(data_buffer, index=False, header=False, sep='\t')
data_buffer.seek(0)

# Ejecuto el COPY command para insertar los datos
copy_query = """
    COPY {} (Date, "Open", High, Low, Close, Volume, Dividends, Stock_Splits)
    FROM STDIN
    DELIMITER '\t'
    NULL ''
""".format(table_name)

with connection.cursor() as cursor:
    cursor.copy_expert(copy_query, data_buffer)


