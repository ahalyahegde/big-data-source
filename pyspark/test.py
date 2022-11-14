
# Databricks notebook source
# DBTITLE 1,Read from CSV file
csv_records = spark.read.format('csv').load('dbfs:/FileStore/raw_dnb_flat_test_samples.csv', header=True, inferSchema=True)
#csv_records.show(10)
csv_records.display()

# COMMAND ----------

# DBTITLE 1,Select operation
subset = csv_records.select('transactionDetail_transactionID', 'transactionDetail_transactionTimestamp', 'organization_telephone_0_isdCode')
subset.show()


# Using alias as the columns names are long
from pyspark.sql.functions import col
subset = subset.select(col('transactionDetail_transactionID').alias('trans_id'), 
                            col('transactionDetail_transactionTimestamp').alias('trans_stamp'), 
                            col('organization_telephone_0_isdCode').alias('isd_code'))
subset.show()

# COMMAND ----------

# DBTITLE 1,Add Column
from pyspark.sql.functions import date_format

new_column = subset.withColumn('created_year', date_format("trans_stamp", 'yyyy'))
new_column.show()

# COMMAND ----------

# DBTITLE 1,Create a partitioned table
partiotned_table = new_column.write.format('delta') \
                                   .mode('overwrite') \
                                   .partitionBy('created_year') \
                                   .saveAsTable('pyspark_trial_table')



# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/pyspark_trial_table/created_year=2022/

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from pyspark_trial_table

# COMMAND ----------

# DBTITLE 1,Update operation
from pyspark.sql import functions as F
updated_df = new_column.withColumn('created_year', F.when(new_column['isd_code'] == '44', '2021')
                                                    .otherwise(new_column['created_year'])
                                  )
updated_df.show()

# COMMAND ----------



