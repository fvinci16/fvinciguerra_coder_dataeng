Proyecto de extracción desde API yahoo a una base de datos en Amazon Redshift-

Para correr este proyecto es necesario tener las siguientes librerias instaladas:
yfinance (api de yahoo)
redshift_connector (connector a redshift)
pandas (manejo del dataframe para control de duplicidad y calculo de métricas)

Los datos de conexión se encuentran variabilizados y se ingresan desde el .py "Parametros_conexión"

----

Para inserción de datos bulk es posible utilizar el método COPY y tener un bucket S3 configurado. Ejemplo del código si tuvieramos acceso al bucket:

import boto3

# Export DataFrame to CSV file
csv_file = 'stock_data.csv'
hist[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits']].to_csv(csv_file, index=False, header=False)

# Upload CSV file to S3 bucket
s3 = boto3.client('s3')
bucket_name = 'your_bucket_name'  # Replace with your S3 bucket name
s3.upload_file(csv_file, bucket_name, csv_file)

# Copy data from S3 bucket into the table
copy_query = f"""
    COPY {my_schema}.stock_data
    FROM 's3://{bucket_name}/{csv_file}'
    DELIMITER ','
    CSV;
"""
