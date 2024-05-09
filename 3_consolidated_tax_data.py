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

# bad_zips = df.groupBy('situs_zip').count().filter(col('count')==1).select('situs_zip')
# raw_100 = read_func('bronze/raw_100')
# ans = df.join(bad_zips,on='situs_zip').join(raw_100, on='situs_zip').select(df['*'], bad_zips['*'], raw_100['OWNERS_ZIP_CODE'])
# ans.show()

# df = (
#     df
#     .withColumn(
#         'situs_zip',
#         when(col('situs_zip')==85052, lit(85045))
#         .when(col('situs_zip')==85220, lit(85120))
#         .when(col('situs_zip')==85269, lit(85249))
#         .when(col('situs_zip')==85056, lit(85045))
#         .when(col('situs_zip')==85364, lit(85354))
#         .when(col('situs_zip')==85067, lit(85087))
#         .when(col('situs_zip')==85539, lit(85339))
#         .when(col('situs_zip')==85055, lit(85045))
#         .when(col('situs_zip')==85038, lit(85020))
#         .when(col('situs_zip')==85235, lit(85234))
#         .when(col('situs_zip')==85039, lit(85020))
#         .when(col('situs_zip')==85358, lit(85390))
#         .when(col('situs_zip')==85626, lit(85262))
#         .when(col('situs_zip')==85389, lit(85387))
#         .when(col('situs_zip')==85005, lit(85008))
#         .when(col('situs_zip')==85328, lit(85326))
#         .when(col('situs_zip')==85036, lit(85020))
#         .when(col('situs_zip')==85385, lit(85388))
#         .when(col('situs_zip')==85252, lit(85253))
#         .otherwise(col('situs_zip'))
#     )
# )


























