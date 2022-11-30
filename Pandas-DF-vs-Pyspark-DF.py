# Databricks notebook source
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, LongType, TimestampType, DateType

# COMMAND ----------

psdf_event_04 = pd.read_csv('/dbfs/mnt/npdac-projects-telco-paas/AIM/data_sources/Events_Data_2022/unzipped/2022-04.csv',
                           parse_dates=[1,2,3,4,5],
                           dtype={'DUPLICATE_COUNT':np.float64
                                  ,'RECEIVED_ON_CI_DOWNTIME':np.float64
                                  ,23:"object"
                                  ,24:"object"
                                  ,25:"object"})

# COMMAND ----------

display(psdf_event_04.loc[:,"SEVERITY"].unique())

# COMMAND ----------

display(spark.createDataFrame(psdf_event_04).select("SEVERITY").distinct())

# COMMAND ----------

df_event_04 = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("multiLine","true") \
                    .csv('/mnt/npdac-projects-telco-paas/AIM/data_sources/Events_Data_2022/unzipped/2022-04.csv')


# COMMAND ----------

display(df_event_04.select("SEVERITY").distinct())

# COMMAND ----------


