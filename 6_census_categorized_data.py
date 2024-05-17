from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.parcels_lib import *


df = read_func('gold/categorized_tax_data_2023')
census_df = read_func('source/qgis_census_tracts.csv')
raw_100 = (
    read_func('bronze/raw_100')
    .withColumn('legal_class', concat(col('LAND_1ST'), lit('.'), col('LAND_RATIO_1ST')))
    .select(
        'parcel_no',
        'legal_class'
    )
)

# remove instances where 1 APN maps to multiple census tracts
dup_apns = (
    census_df
    .groupBy('apn')
    .count()
    .filter(col('count')>1)
    .select('APN').distinct()
)

clean_census_df = (
    census_df
    .join(dup_apns, on='apn', how='leftanti')
    .withColumnRenamed('apn', 'parcel_no')
    .select('parcel_no', 'TRACTCE').distinct()
)

join_df = (
    df
    .join(clean_census_df, on='parcel_no', how='left')
    .join(raw_100, on='parcel_no', how='left')
)

print(join_df.count())

write_func(join_df, f'gold/census_categorized_tax_data_2023')
