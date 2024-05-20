from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lib.parcels_lib import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName('consolidate_tax_data') \
    .getOrCreate()

# add tax/acre col
df = read_func('bronze/tax_data_2023')
df = df.drop('results')
df = df.withColumn('tax/acre', col('tax_2023')/col('acres_clean'))

write_func(df, 'bronze/consolidated_tax_data_2023')
