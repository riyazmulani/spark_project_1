from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from azure.storage.blob import BlobServiceClient  #For interacting with Azure Blob Storage
import io # For in-memory buffer management.

"""What is the Task ? -> reads data from an Oracle database, processes it using Spark,
converts the processed data to a CSV format, and uploads it to Azure Data Lake Storage (ADLS)"""
class common_config:
    """
    Why JDBC driver needed ? -> Initializes a Spark session with a JDBC driver for Oracle.
    This allows Spark to communicate with the Oracle database.
    """
    spark = SparkSession \
            .builder \
            .config("spark.jars", r"C:\Users\Riyaz\Desktop\Theory\ojdbc8.jar") \
            .getOrCreate()

    def generate_dataframe(self,table_number):
        # Oracle connection parameters
        host = "localhost"
        port = "1521"
        schema = "XE"
        URL = f"jdbc:oracle:thin:@{host}:{port}:{schema}"
        query = f"select * from datafile30 where TABLE_NUMBER='{table_number}'"

        # Read data from Oracle database
        oracle_df = (self.spark.read.format("jdbc")
                     .option("url", URL)
                     .option("query", query)
                     .option("user", "sys as sysdba")
                     .option("password", "Riyaz")
                     .option("driver", "oracle.jdbc.driver.OracleDriver")
                     .load()
                     )

        df = oracle_df.withColumn("EFFECTIVE_DATE",col("EFFECTIVE_DATE").cast("string"))\
        .withColumn("EXPIRATION_DATE",col("EXPIRATION_DATE").cast("string"))


        return df



    def upload_to_adlsgen2(self,df,file_name):
        """ toPandas ? -> Converts the Spark DataFrame to a pandas DataFrame.
            io.StringIO()? -> Creates an in-memory text buffer.
            to_csv: Writes -> the pandas DataFrame to the buffer in CSV format.
            csv_content: Retrieves the CSV content from the buffer.
        """
        pandas_df = df.toPandas() # Converting DF into pandas DF

        csv_buffer = io.StringIO()     # Create an in-memory buffer

        pandas_df.to_csv(csv_buffer, index=False)       # Write the DataFrame to the buffer

        csv_content = csv_buffer.getvalue()         # Get the CSV content


        access_key = "i23FGYyUqVXbdxX4ndLSqTy+syxiFarJGFQZ9dM742EV1XWKVRFbOvMFBP3Y7t6AosM1ndtp5Td5+AStOXbPQA=="
        blob_name = f"{file_name}_.csv"
        storage_account_name = "covidrreporting"
        container_name = "datafile"


        # Initialize the BlobServiceClient
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={access_key};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get a client to interact with the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Upload the CSV content from the in-memory buffer
        blob_client.upload_blob(csv_content, blob_type="BlockBlob",overwrite=True)