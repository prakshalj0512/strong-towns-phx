from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lib.parcels_lib import *
from lib.parcel_udfs import *


# Initialize SparkSession
spark = SparkSession.builder \
    .appName('backfill_tax_data') \
    .getOrCreate()

################
# table read
################
consolidated_tax_data_2023 = read_func('../../Secured_Master/consolidated_tax_data_2023')

################
# parcels to rescrape
################
valid_taxes_df = consolidated_tax_data_2023.filter(col('tax_2023').rlike(r'^[0-9]+(\.[0-9]+)?$'))

need_to_rescrape = consolidated_tax_data_2023.join(valid_taxes_df, on='parcel_no', how='leftanti')

print(f'NEED TO rescrape: {need_to_rescrape.count()}')
need_to_rescrape=need_to_rescrape.repartition(24)

################
# rescrape logic
################
rescraped_taxes = (
    need_to_rescrape
    .rdd.map(lambda row: (row['parcel_no'], make_request(row['parcel_no']))).toDF(['parcel_no', 'response_code'])
)

################
# merge old valid dataset with new rescraped
################

ans = consolidated_tax_data_2023.join(rescraped_taxes,on='parcel_no')

rescraped_df = (
    ans
    .select(
        'parcel_no',
        'situs_zip',
        'owners_name',
        'property_use_code_major',
        'property_use_code',
        'acres_clean',
        col('response_code').alias('tax_2023')
    )
    .withColumn('tax/acre', col('tax_2023')/col('acres_clean'))
)

backfilled_tax_data_df = valid_taxes_df.union(rescraped_df)

write_func(backfilled_tax_data_df, 'bronze/backfilled_tax_data_2023')
