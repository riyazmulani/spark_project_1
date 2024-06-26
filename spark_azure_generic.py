from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from sql_spark_azure import commonutils_sql_spark

if __name__ == '__main__':
    cmn_ut = commonutils_sql_spark.common_config()

    # condition 1 => fetch table number =   BGII(LC) & count should be 64
    df1 = cmn_ut.generate_dataframe("BGII(LC)")
    df1.show()
    co = df1.count()
    print(co)

    TABLE1_df = df1 \
        .withColumnRenamed("Key1", "BGIITerritory") \
        .withColumnRenamed("Key2", "BGIICoverage") \
        .withColumnRenamed("Key3", "BGIISymbol") \
        .withColumnRenamed("factor", "FactorBGII") \
        .drop("key4")
    TABLE1_df.show()

    cmn_ut.upload_to_adlsgen2(TABLE1_df, "Table1")
    # count_records1 = TABLE1_df.count()
    # print(f"Count of records where tablenumber = 'BGII(LC)': {count_records1}")

    # condition 2 => fetch table number = B.(1)(LC) & Key1 – BGIITerritory, Key2—BGIICoverage, Key3—BGIISymbol, factor--
    # FactorBGII

    df2 = cmn_ut.generate_dataframe('B.(1)(LC)')

    TABLE2_df = df2 \
        .withColumnRenamed("Key1", "coverage") \
        .withColumnRenamed("factor", "FactorC2LC") \
        .drop("key4")

    TABLE2_df.show()

    cmn_ut.upload_to_adlsgen2(TABLE2_df, "Table2")

    #     # count_records2 = TABLE2_df.count()
    #     # print(f"Count of records where tablenumber = 'B.(1)(LC)': {count_records2}")

    # #condition 3 => fetch table number = C.(2)(LC)

    df3 = cmn_ut.generate_dataframe('C.(2)(LC)')

    TABLE3_df = df3 \
        .withColumnRenamed("Key1", "coverage") \
        .drop("key2") \
        .drop("Key3") \
        .withColumnRenamed("factor", "FactorC2LC") \
        .drop("key4")

    TABLE3_df.show()

    cmn_ut.upload_to_adlsgen2(TABLE3_df, "Table3")

    #     # count_records3 = TABLE3_df.count()
    #     # print(f"Count of records where tablenumber = 'C.(2)(LC)': {count_records3}")

    # condition 4 => fetch table number = 85.(LC) & Key1 – Class code, Key2—Coverage85LC, Key3—Symbo85LC,
    # Key4 – Constuction Code, Factor—Factor85LC

    df4 = cmn_ut.generate_dataframe('C.(2)(LC)')

    TABLE4_df = df4 \
        .withColumnRenamed("Key1", "class_code") \
        .withColumnRenamed("Key2", "coverage85LC") \
        .withColumnRenamed("Key3", "Constuction_Code") \
        .withColumnRenamed("factor", "Factor85LC") \
        .drop("key4")

    table4_df = TABLE4_df.select("class_code", "coverage85LC", "Constuction_Code", "Factor85LC")
    #
    # TABLE4_df.show()
    #
    # cmn_ut.upload_to_adlsgen2(TABLE4_df, "Table4")

    #     # count_records4 = TABLE4_df.count()
    #     # print(f"Count of records where tablenumber = '85.(LC)': {count_records4}")

    # #condition 5 => fetch table number = 85.(LC) & fetch columns Key1 – Territory, Factor—Factor85TERR

    df5 = cmn_ut.generate_dataframe('85.TerrMult(LC)')

    TABLE5_df = df5 \
        .withColumnRenamed("Key1", "Territory") \
        .withColumnRenamed("factor", "Factor85TERR") \
        .drop("key2") \
        .drop("key3") \
        .drop("key4")

    table5_df = TABLE5_df.select("Territory", "Factor85TERR")

    TABLE5_df.show()

    cmn_ut.upload_to_adlsgen2(TABLE5_df, "Table5")

    # count_records5 = TABLE5_df.count()
    # print(f"Count of records where tablenumber = '85.TerrMult(LC)': {count_records5}")