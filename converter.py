import os
os.environ["JAVA_HOME"] = "/home/rachit_bedi1/jdk1.8.0_251"

import findspark
findspark.init()

import pandas as pd
import tabula
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import unix_timestamp, from_unixtime


spark = SparkSession.builder.master("local[*]").getOrCreate()


def pdf_to_csv(pdf_path, password, file_name, upload_folder_path):
    # readingthepdfbody
    dfs = tabula.read_pdf(pdf_path, password=password, pages="all", lattice=True, multiple_tables=True,
                          pandas_options={'header': None}, )

    # readingthepdfforheaderdetails
    dfs1 = tabula.read_pdf(pdf_path, password=password, stream=True, pages=1, area=[0, 0, 50, 100], relative_area=True,
                           pandas_options={'header': None}, )

    # Extracttheheaderdetailsinaloop
    finaldf1 = []
    for df in dfs1[0:]:
        finaldf1.append(df)

    finaldf1 = pd.concat(finaldf1)

    # Extractstatementdetails
    Namelist = finaldf1.iloc[2, 0].split(' ', 3)[2:]
    Name = ' '.join(str(e) for e in Namelist)
    Mobile = finaldf1.iloc[3, 0].split(" ")[-1]
    Email = finaldf1.iloc[4, 0].split(" ")[-1]

    # Extractthemainbodyofstatement

    list1 = ['Transaction_Id', 'Date_time', 'Details', 'Status', 'Paid_in', 'Withdrawn', 'Balance']
    finaldf = []
    for df in dfs[1:]:
        finaldf.append(df)

    finaldf = pd.concat(finaldf)
    # datapreparationsteps

    finaldf1 = finaldf.drop(finaldf.columns[7], axis=1).rename(columns=finaldf.iloc[0]).drop(finaldf.index[0])
    finaldf1 = finaldf1.rename(columns=dict(zip(finaldf1.columns, list1)))

    # Thislineofcodecreatesasparkdataframesfrompandas
    finaldf2 = spark.createDataFrame(finaldf1.astype(str))
    # Thislineofcodecreatescolumnsbasedonsomewhenconditions
    # Additionally itsassignssomecolumnsbaseonsomequerydefineabove
    finaldf3 = finaldf2.withColumn('Amount', F.when((F.col("Withdrawn") == 'nan'), col("Paid_in")).otherwise(
        col("Withdrawn"))).withColumn('Direction', F.when((F.col("Withdrawn") == 'nan'), "Paid_in")
                                      .otherwise("Withdrawn")).withColumn('Customer_Name', F.lit(Name)).withColumn(
        'Mobile',
        F.lit(
            Mobile)).withColumn(
        'Email', F.lit(Email))

    # Thislineselectsthefieldstobexported
    finaldf4 = finaldf3.select('Transaction_Id', 'Customer_Name',
                               from_unixtime(unix_timestamp(finaldf3.Date_time, 'yyyy-MM-dd HH:mm:ss')).alias(
                                   'date_parse'),
                               'Details', 'Status', 'Amount', 'Balance', 'Direction', 'Mobile', 'Email')

    # theexportpartwillbereplacedbydownload
    # ifwecanmakethefiledeleteondownloadcompletion

    finaldf4.toPandas().to_csv(os.path.join(upload_folder_path, file_name + ".csv"), header=True, index=False)
    print("completed")
    return file_name + ".csv"
