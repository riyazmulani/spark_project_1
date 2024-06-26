import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *



# Build the Spark sessionjar
spark = SparkSession.builder.config("spark.jars",r"C:\Users\Riyaz\Desktop\Theory\ojdbc8.jar").master("local[*]").getOrCreate()


# Oracle connection parameters
host = "localhost"
port = "1521"
schema = "XE"
URL = f"jdbc:oracle:thin:@{host}:{port}:{schema}"

query = "select * from dataset1"

# Read data from Oracle database
dataset1_df = (spark.read.format("jdbc")
    .option("url", URL)
    .option("query", query)
    # .option("dbtable", "datafile30")
    .option("user", "sys as sysdba")
    .option("password", "Riyaz")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()
)

# Print schema and show data
dataset1_df.printSchema()
dataset1_df.show()


# # create table 1
# print("_cond2_")
#
# table_df = spark.read \
#         .format("jdbc") \
#         .option("url", URL) \
#         .option("query", "select * from datafile30 where table_number = 'BGII(LC)'") \
#         .option("user", "sys as sysdba") \
#         .option("password", "Riyaz") \
#         .option("driver", "oracle.jdbc.driver.OracleDriver") \
#         .load()
# table_df.show()
#
#
#
# # Transformation on Table – 1 -- BGII(LC)
# print("___cond3-----")
# df_BGII_LC = oracle_df.filter(col('TABLE_NUMBER').ilike('BGII(LC)'))
#
# df_BGII_LC = df_BGII_LC.withColumnRenamed("KEY1","BGIITerritory")\
#         .withColumnRenamed("KEY2","BGIICoverage")\
#         .withColumnRenamed("KEY3","BGIISymbol")\
#         .withColumnRenamed("FACTOR","FactorBGII")
#
#
# df_BGII_LC.show()
#
#
# #creating tempview for sql queries
# df_BGII_LC.createOrReplaceTempView("BGII_LC")
# spark.sql("select * from BGII_LC").show()
#
#
# # Transformation on Table – 2 -- B.(1)(LC)
# print("---table2 transformation--------")
#
# df_B1_LC = oracle_df.filter(col('TABLE_NUMBER').ilike('B.(1)(LC)'))
# df_B1_LC = df_B1_LC.withColumnRenamed("FACTOR","FactorB1LC")
#
# df_B1_LC.show()
#
# df_B1_LC.createOrReplaceTempView("B1_LC")
#
#
#
# # Transformation on Table – 3 -- C.(2)(LC)
#
# print("---table3 transformation--------")
# df_C2_LC = oracle_df.filter(col('TABLE_NUMBER').ilike('C.(2)(LC)'))
# df_C2_LC = df_C2_LC.withColumnRenamed("KEY1","Coverage")\
#                        .withColumnRenamed("FACTOR","FactorC2LC")
#
# df_C2_LC.show()
#
# df_C2_LC.createOrReplaceTempView("C2_LC")
#
#
#
# # Transformation on Table – 4 -- 85.(LC)
# print("---table4 transformation--------")
# df_85_LC = oracle_df.filter(col('TABLE_NUMBER').ilike('85.(LC)'))
#
# df_85_LC = df_85_LC.withColumnRenamed("KEY1","Class_code")\
#                        .withColumnRenamed("KEY2","Coverage85LC")\
#                        .withColumnRenamed("KEY3","Symbo85LC")\
#                        .withColumnRenamed("KEY4","Constuction_Code")\
#                        .withColumnRenamed("FACTOR","Factor85LC")
# df_85_LC.show()
#
# df_85_LC.createOrReplaceTempView("85_LC")
#
#
#
# # Transformation on Table – 5 -- 85.TerrMult(LC)
# print("---table5 transformation--------")
#
#
# df_85_TerrMult_LC = oracle_df.filter(col('TABLE_NUMBER').ilike('85.TerrMult(LC)'))
#
# df_85_TerrMult_LC = df_85_TerrMult_LC.withColumnRenamed("KEY1","Territory")\
#                                         .withColumnRenamed("FACTOR","Factor85TERR")
#
# df_85_TerrMult_LC.show()